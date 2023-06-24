<?php

declare(strict_types=1);

namespace Localizationteam\L10nmgr\Controller;

/***************************************************************
 * Copyright notice
 * (c) 2006 Kasper Skårhøj <kasperYYYY@typo3.com>
 * All rights reserved
 * This script is part of the TYPO3 project. The TYPO3 project is
 * free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * The GNU General Public License can be found at
 * http://www.gnu.org/copyleft/gpl.html.
 * This script is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * This copyright notice MUST APPEAR in all copies of the script!
 ***************************************************************/

use Exception;
use Localizationteam\L10nmgr\Model\CatXmlImportManager;
use Localizationteam\L10nmgr\Model\Dto\EmConfiguration;
use Localizationteam\L10nmgr\Model\L10nBaseService;
use Localizationteam\L10nmgr\Model\L10nConfiguration;
use Localizationteam\L10nmgr\Model\MkPreviewLinkService;
use Localizationteam\L10nmgr\Model\TranslationData;
use Localizationteam\L10nmgr\Model\TranslationDataFactory;
use Localizationteam\L10nmgr\View\CatXmlView;
use Localizationteam\L10nmgr\View\ExcelXmlView;
use Localizationteam\L10nmgr\View\L10nConfigurationDetailView;
use Localizationteam\L10nmgr\View\L10nHtmlListView;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use TYPO3\CMS\Backend\Configuration\TranslationConfigurationProvider;
use TYPO3\CMS\Backend\Routing\Exception\ResourceNotFoundException;
use TYPO3\CMS\Backend\Routing\Exception\RouteNotFoundException;
use TYPO3\CMS\Backend\Routing\Router;
use TYPO3\CMS\Backend\Routing\UriBuilder;
use TYPO3\CMS\Backend\Template\ModuleTemplate;
use TYPO3\CMS\Backend\Utility\BackendUtility;
use TYPO3\CMS\Core\Core\Environment;
use TYPO3\CMS\Core\Http\HtmlResponse;
use TYPO3\CMS\Core\Http\Response;
use TYPO3\CMS\Core\Http\Stream;
use TYPO3\CMS\Core\Imaging\Icon;
use TYPO3\CMS\Core\Imaging\IconFactory;
use TYPO3\CMS\Core\Mail\MailMessage;
use TYPO3\CMS\Core\Messaging\AbstractMessage;
use TYPO3\CMS\Core\Messaging\FlashMessage;
use TYPO3\CMS\Core\Messaging\FlashMessageRendererResolver;
use TYPO3\CMS\Core\Utility\ExtensionManagementUtility;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Core\Utility\PathUtility;
use TYPO3\CMS\Fluid\View\StandaloneView;

/**
 * l10nmgr module Configuration Manager
 *
 * @author Kasper Skårhøj <kasperYYYY@typo3.com>
 * @author Daniel Zielinski <d.zielinski@l10ntech.de>
 * @author Daniel Pötzinger <poetzinger@aoemedia.de>
 * @author Fabian Seltmann <fs@marketing-factory.de>
 * @author Andreas Otto <andreas.otto@dkd.de>
 * @author Jo Hasenau <info@cybercraft.de>
 * @author Stefano Kowalke <info@arroba-it.de>
 */

/**
 * Translation management tool
 *
 * @author Kasper Skaarhoj <kasperYYYY@typo3.com>
 */
class LocalizationManager extends BaseModule
{
    /**
     * @var int Default language to export
     */
    protected int $sysLanguage = 0; // Internal

    /**
     * @var int Forced source language to export
     */
    public int $previewLanguage = 0;

    /**
     * ModuleTemplate Container
     *
     * @var ModuleTemplate
     */
    protected ModuleTemplate $moduleTemplate;

    /**
     * @var StandaloneView
     */
    protected StandaloneView $view;

    /**
     * The name of the module
     *
     * @var string
     */
    protected string $moduleName = 'LocalizationManager';

    /**
     * @var IconFactory
     */
    protected IconFactory $iconFactory;

    /**
     * @var array
     */
    protected array $pageinfo;

    /**
     * @var EmConfiguration
     */
    protected EmConfiguration $emConfiguration;

    protected array $settings = [
        'across' => 'acrossL10nmgrConfig.dst',
        'dejaVu' => 'dejaVuL10nmgrConfig.dvflt',
        'memoq' => 'memoQ.mqres',
        'memoq2013-2014' => 'XMLConverter_TYPO3_l10nmgr_v3.6.mqres',
        'transit' => 'StarTransit_XML_UTF_TYPO3.FFD',
        'sdltrados2007' => 'SDLTradosTagEditor.ini',
        'sdltrados2009' => 'TYPO3_l10nmgr.sdlfiletype',
        'sdltrados2011-2014' => 'TYPO3_ConfigurationManager_v3.6.free.sdlftsettings',
        'sdlpassolo' => 'SDLPassolo.xfg',
    ];

    public function __construct()
    {
        $this->emConfiguration = GeneralUtility::makeInstance(EmConfiguration::class);
        $this->iconFactory = GeneralUtility::makeInstance(IconFactory::class);
        $this->moduleTemplate = GeneralUtility::makeInstance(ModuleTemplate::class);
        $this->getLanguageService()
            ->includeLLFile('EXT:l10nmgr/Resources/Private/Language/Modules/LocalizationManager/locallang.xlf');
        $this->MCONF = [
            'name' => $this->moduleName,
        ];
    }

    /**
     * Injects the request object for the current request or subrequest
     * Then checks for module functions that have hooked in, and renders menu etc.
     *
     * @return ResponseInterface the response with the content
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    public function mainAction(): ResponseInterface
    {
        $this->init();

        // Checking for first level external objects
        $this->checkExtObj();

        // Checking second level external objects
        $this->checkSubExtObj();

        switch ($this->MOD_SETTINGS['action'] ?? '') {
            case 'inlineEdit':
            case 'link':
                // TODO: Rename this to main()
                $this->mainNew();
                break;
            default:
                // TODO: Remove this when done
                $this->main();
                break;
        }

        $this->moduleTemplate->setContent($this->content);

        return new HtmlResponse($this->moduleTemplate->renderContent());
    }

    /**
     * Initializes the Module
     */
    public function init(): void
    {
        $this->getBackendUser()->modAccess($this->MCONF);
        parent::init();
    }

    /**
     * Main function of the module. Write the content to
     *
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    protected function mainNew()
    {
        // Get language to export/import
        $this->sysLanguage = (int)($this->MOD_SETTINGS['lang'] ?? 0);

        // Javascript
        $this->moduleTemplate->addJavaScriptCode(
            'jumpToUrl',
            '
function jumpToUrl(URL) {
window.location.href = URL;
return false;
}
'
        );

        $this->moduleTemplate->setTitle('L10N Manager');

        $this->moduleTemplate->setForm('<form action="" method="post" enctype="multipart/form-data">');

        // Find l10n configuration record
        $l10nConfiguration = $this->getL10NConfiguration();
        if ($l10nConfiguration->isLoaded()) {
            // Setting page id
            $this->id = $l10nConfiguration->getPid();
            $this->perms_clause = $this->getBackendUser()->getPagePermsClause(1);
            $this->pageinfo = BackendUtility::readPageAccess($this->id, $this->perms_clause);
            $access = is_array($this->pageinfo);
            if ($this->id && $access) {
                $action = (string)($this->MOD_SETTINGS['action'] ?? '');
                $title = $this->MOD_MENU['action'][$action] ?? '';

                $addParams = sprintf('&srcPID=%d&exportUID=%d', rawurlencode(GeneralUtility::_GET('srcPID')), $l10nConfiguration->getId());
                $selectMenus = [];
                $selectMenus[] = self::getFuncMenuNew(
                    $this->id,
                    'SET[action]',
                    $action,
                    $this->MOD_MENU['action'] ?? [],
                    '',
                    $addParams,
                    $this->getLanguageService()->getLL('general.export.choose.action.title')
                );

                $selectMenus[] = self::getFuncMenuNew(
                    $this->id,
                    'SET[lang]',
                    (string)$this->sysLanguage,
                    $this->MOD_MENU['lang'] ?? [],
                    '',
                    $addParams,
                    $this->getLanguageService()->getLL('export.overview.targetlanguage.label')
                );

                $checkBoxes = [];
                $checkBoxes[] = self::getFuncCheckNew(
                    $this->id,
                    'SET[onlyChangedContent]',
                    $this->MOD_SETTINGS['onlyChangedContent'] ?? '',
                    '',
                    $addParams,
                    '',
                    $this->getLanguageService()->getLL('export.xml.new.title')
                );
                $checkBoxes[] = self::getFuncCheckNew(
                    $this->id,
                    'SET[noHidden]',
                    $this->MOD_SETTINGS['noHidden'] ?? '',
                    '',
                    $addParams,
                    '',
                    $this->getLanguageService()->getLL('export.xml.noHidden.title')
                );

                // Render content:
                $userCanEditTranslations = count($this->MOD_MENU['lang'] ?? []) > 0;

                $moduleContent = [];
                if ($userCanEditTranslations) {
                    $moduleContent = $this->moduleContentNew($l10nConfiguration);
                }

                // Create and render view to show details for the current L10N Manager configuration
                $configurationTable = $this->renderConfigurationTable($l10nConfiguration);

                $this->view = $this->getFluidTemplateObject();
                $this->view->assignMultiple([
                    'title' => $title,
                    'selectMenues' => $selectMenus,
                    'checkBoxes' => $checkBoxes,
                    'userCanEditTranslations' => $userCanEditTranslations,
                    'moduleAction' => $action,
                    'moduleContent' => $moduleContent,
                    'configurationTable' => $configurationTable,
                    'isRteInstalled' => ExtensionManagementUtility::isLoaded('rte_ckeditor'),
                ]);

                $this->content .= $this->view->render();
            }
        }
    }

    /**
     * Main function of the module. Write the content to
     *
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    protected function main()
    {
        // Get language to export/import
        $this->sysLanguage = (int)$this->MOD_SETTINGS['lang'];

        // Javascript
        $this->moduleTemplate->addJavaScriptCode(
            'jumpToUrl',
            '
function jumpToUrl(URL) {
window.location.href = URL;
return false;
}
'
        );

        $this->moduleTemplate->setTitle('L10N Manager');

        $this->moduleTemplate->setForm('<form action="" method="post" enctype="multipart/form-data">');

        // Find l10n configuration record
        $L10nConfiguration = $this->getL10NConfiguration();
        if ($L10nConfiguration->isLoaded()) {
            // Setting page id
            $this->id = (int)$L10nConfiguration->getData('pid');
            $forcedSourceLanguage = (int)$L10nConfiguration->getData('forcedSourceLanguage');
            if ($forcedSourceLanguage > 0) {
                $this->previewLanguage = $forcedSourceLanguage;
            }
            $this->perms_clause = $this->getBackendUser()->getPagePermsClause(1);
            $this->pageinfo = BackendUtility::readPageAccess($this->id, $this->perms_clause);
            $access = is_array($this->pageinfo);
            if ($this->id && $access) {
                $action = (string)($this->MOD_SETTINGS['action'] ?? '');
                $title = $this->MOD_MENU['action'][$action];

                $this->content .= '<div class="panel panel-default expanded">
    <div class="panel-heading" role="tab" id="headingL10nmgrPanel">
        <h2 class="panel-title">' . $title . '
            <a role="button" data-toggle="collapse" data-bs-toggle="collapse" href="#l10nmgrPanel" aria-expanded="true" aria-controls="l10nmgrPanel" class="pull-right"><span class="caret"></span></a>
        </h2>
    </div>
    <div id="l10nmgrPanel" class="panel-collapse collapse in show" role="tabpanel" aria-labelledby="headingL10nmgrPanel">
        <div class="panel-body">
            <div class="row">';
                $this->content .= '
    <div class="col-md-6">
        <div class="form">
            <div class="form-section">' .
                    self::getFuncMenu(
                        $this->id,
                        'SET[action]',
                        $action,
                        $this->MOD_MENU['action'] ?? [],
                        '',
                        '&srcPID=' . rawurlencode(GeneralUtility::_GET('srcPID')) . '&exportUID=' . $L10nConfiguration->getId(),
                        $this->getLanguageService()->getLL('general.export.choose.action.title')
                    ) .
                    self::getFuncMenu(
                        $this->id,
                        'SET[lang]',
                        (string)$this->sysLanguage,
                        $this->MOD_MENU['lang'] ?? [],
                        '',
                        '&srcPID=' . rawurlencode(GeneralUtility::_GET('srcPID')) . '&exportUID=' . $L10nConfiguration->getId(),
                        $this->getLanguageService()->getLL('export.overview.targetlanguage.label'),
                        (string)$this->previewLanguage
                    ) .
                    '</div><div class="form-section">' .
                    self::getFuncCheck(
                        $this->id,
                        'SET[onlyChangedContent]',
                        $this->MOD_SETTINGS['onlyChangedContent'] ?? '',
                        '',
                        '&srcPID=' . rawurlencode(GeneralUtility::_GET('srcPID')) . '&exportUID=' . $L10nConfiguration->getId(),
                        '',
                        $this->getLanguageService()->getLL('export.xml.new.title')
                    ) .
                    self::getFuncCheck(
                        $this->id,
                        'SET[noHidden]',
                        $this->MOD_SETTINGS['noHidden'] ?? '',
                        '',
                        '&srcPID=' . rawurlencode(GeneralUtility::_GET('srcPID')) . '&exportUID=' . $L10nConfiguration->getId(),
                        '',
                        $this->getLanguageService()->getLL('export.xml.noHidden.title')
                    ) .
                    '</div></div></div>';
                // Render content:
                if (!count($this->MOD_MENU['lang'])) {
                    $this->content .= '<div><h2>ERROR<h2>' . $this->getLanguageService()->getLL('general.access.error.title') . '</div>';
                } else {
                    $this->moduleContent($L10nConfiguration);
                }
            }
        }
    }

    /**
     * Returns a selector box "function menu" for a module
     * Requires the JS function jumpToUrl() to be available
     * See Inside TYPO3 for details about how to use / make Function menus
     *
     * @param mixed $mainParams The "&id=" parameter value to be sent to the module, but it can be also a parameter array which will be passed instead of the &id=...
     * @param string $elementName The form elements name, probably something like "SET[...]
     * @param string $currentValue The value to be selected currently.
     * @param array $menuItems An array with the menu items for the selector box
     * @param string $script The script to send the &id to, if empty it's automatically found
     * @param string $addParams Additional parameters to pass to the script.
     * @param string $label
     *
     * @return array
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    public static function getFuncMenuNew(
        $mainParams,
        string $elementName,
        string $currentValue,
        array $menuItems,
        string $script = '',
        string $addParams = '',
        string $label = ''
    ): array {
        if (empty($menuItems)) {
            return [];
        }
        $scriptUrl = self::buildScriptUrl($mainParams, $addParams, $script);
        $options = [];
        foreach ($menuItems as $value => $text) {
            $options[] = [
                'value' => htmlspecialchars((string)$value),
                'selected' => ($currentValue === (string)$value),
                'label' => htmlspecialchars((string)$text, ENT_COMPAT, 'UTF-8', false),
            ];
        }
        $label = $label !== '' ? htmlspecialchars($label) : '';
        if (count($options) > 0) {
            $onChange = 'jumpToUrl(' . GeneralUtility::quoteJSvalue($scriptUrl . '&' . $elementName . '=') . '+this.options[this.selectedIndex].value,this);';

            return [
                'label' => $label,
                'elementName' => $elementName,
                'onChange' => $onChange,
                'options' => $options,
            ];
        }

        return [];
    }

    /**
     * @param mixed $mainParams
     * @param string $elementName
     * @param mixed $currentValue
     * @param array $menuItems
     * @param string $script
     * @param string $addParams
     * @param string $label
     * @param string $previewLanguage
     * @return string
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    public static function getFuncMenu(
        $mainParams,
        string $elementName,
        $currentValue,
        array $menuItems,
        string $script = '',
        string $addParams = '',
        string $label = '',
        string $previewLanguage = ''
    ): string {
        if (empty($menuItems)) {
            return '';
        }
        $scriptUrl = self::buildScriptUrl($mainParams, $addParams, $script);
        $options = [];
        foreach ($menuItems as $value => $text) {
            if ($elementName === 'SET[lang]' && (int)$value === (int)$previewLanguage) {
                continue;
            }
            $options[] = '<option value="' . htmlspecialchars((string)$value) . '"' . ((string)$currentValue === (string)$value ? ' selected="selected"' : '') . '>' . htmlspecialchars(
                $text,
                ENT_COMPAT,
                'UTF-8',
                false
            ) . '</option>';
        }
        $label = $label !== '' ?
            ('<label for="' . $elementName . '">' . htmlspecialchars($label) . '</label><br />') :
            '';
        if (!empty($options)) {
            $onChange = 'jumpToUrl(' . GeneralUtility::quoteJSvalue($scriptUrl . '&' . $elementName . '=') . '+this.options[this.selectedIndex].value,this);';
            return '
	<!-- Function Menu of module -->
<div class="form-group mb-2">' .
                $label .
                '<select class="form-control clear-both" id="' . $elementName . '" name="' . $elementName . '" onchange="' . htmlspecialchars($onChange) . '">
	' . implode('
	', $options) . '
	</select>
	</div>
	';
        }
        return '';
    }

    /**
     * Builds the URL to the current script with given arguments
     *
     * @param mixed $mainParams $id is the "&id=" parameter value to be sent to the module, but it can be also a parameter array which will be passed instead of the &id=...
     * @param string $addParams Additional parameters to pass to the script.
     * @param string $script The script to send the &id to, if empty it's automatically found
     * @return string The completes script URL
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    protected static function buildScriptUrl($mainParams, string $addParams, string $script = ''): string
    {
        if (!is_array($mainParams)) {
            $mainParams = ['id' => $mainParams];
        }
        if (!$script) {
            $script = basename(Environment::getCurrentScript());
        }
        if (GeneralUtility::_GP('route')) {
            /** @var Router $router */
            $router = GeneralUtility::makeInstance(Router::class);
            $route = $router->match(GeneralUtility::_GP('route'));
            /** @var UriBuilder $uriBuilder */
            $uriBuilder = GeneralUtility::makeInstance(UriBuilder::class);
            $scriptUrl = (string)$uriBuilder->buildUriFromRoute($route->getOption('_identifier'));
            $scriptUrl .= $addParams;
        } elseif ($script === 'index.php' && GeneralUtility::_GET('M')) {
            $uriBuilder = GeneralUtility::makeInstance(UriBuilder::class);
            $scriptUrl = $uriBuilder->buildUriFromRoute(GeneralUtility::_GET('M'), $mainParams) . $addParams;
        } else {
            $scriptUrl = $script . '?' . GeneralUtility::implodeArrayForUrl('', $mainParams) . $addParams;
        }
        return $scriptUrl;
    }

    /**
     * Checkbox function menu.
     * Works like ->getFuncMenu() but takes no $menuItem array since this is a simple checkbox.
     *
     * @param mixed $mainParams $id is the "&id=" parameter value to be sent to the module, but it can be also a parameter array which will be passed instead of the &id=...
     * @param string $elementName The form elements name, probably something like "SET[...]
     * @param string $currentValue The value to be selected currently.
     * @param string $script The script to send the &id to, if empty it's automatically found
     * @param string $addParams Additional parameters to pass to the script.
     * @param string $tagParams Additional attributes for the checkbox input tag
     * @param string $label
     * @return array
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     * @see getFuncMenu()
     */
    public static function getFuncCheckNew(
        $mainParams,
        string $elementName,
        string $currentValue,
        string $script = '',
        string $addParams = '',
        string $tagParams = '',
        string $label = ''
    ): array {
        $scriptUrl = self::buildScriptUrl($mainParams, $addParams, $script);
        $onClick = 'jumpToUrl(' . GeneralUtility::quoteJSvalue($scriptUrl . '&' . $elementName . '=') . '+(this.checked?1:0),this);';

        return [
            'onClick' => htmlspecialchars($onClick),
            'elementName' => $elementName,
            'checked' => ($currentValue ? ' checked="checked"' : ''),
            'tagParams' => ($tagParams ? ' ' . $tagParams : ''),
            'label' => htmlspecialchars($label),
        ];
    }

    /**
     * Checkbox function menu.
     * Works like ->getFuncMenu() but takes no $menuItem array since this is a simple checkbox.
     *
     * @param mixed $mainParams $id is the "&id=" parameter value to be sent to the module, but it can be also a parameter array which will be passed instead of the &id=...
     * @param string $elementName The form elements name, probably something like "SET[...]
     * @param string $currentValue The value to be selected currently.
     * @param string $script The script to send the &id to, if empty it's automatically found
     * @param string $addParams Additional parameters to pass to the script.
     * @param string $tagParams Additional attributes for the checkbox input tag
     * @param string $label
     *
     * @return string HTML code for checkbox
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     * @see getFuncMenu()
     */
    public static function getFuncCheck(
        $mainParams,
        string $elementName,
        string $currentValue,
        string $script = '',
        string $addParams = '',
        string $tagParams = '',
        string $label = ''
    ): string {
        $scriptUrl = self::buildScriptUrl($mainParams, $addParams, $script);
        $onClick = 'jumpToUrl(' . GeneralUtility::quoteJSvalue($scriptUrl . '&' . $elementName . '=') . '+(this.checked?1:0),this);';
        return
            '<div class="form-group mb-2">' .
            '<div class="checkbox">
<label>
<input' .
            ' type="checkbox"' .
            ' name="' . $elementName . '"' .
            ($currentValue ? ' checked="checked"' : '') .
            ' onclick="' . htmlspecialchars($onClick) . '"' .
            ($tagParams ? ' ' . $tagParams : '') .
            ' value="1"' .
            ' />&nbsp;' .
            htmlspecialchars($label) .
            '</label>
</div>
</div>';
    }

    /**
     * Creating module content
     *
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    protected function moduleContentNew(L10nConfiguration $l10NConfiguration): array
    {
        $subcontentNew = [];

        switch ($this->MOD_SETTINGS['action'] ?? '') {
            case 'inlineEdit':
            case 'link':
                $subcontentNew = $this->linkOverviewAndOnlineTranslationAction($l10NConfiguration, $subcontentNew);
                break;
            case 'export_excel':
                $subcontentNew = $this->excelExportImportAction($l10NConfiguration) . '</div></div></div></div></div><div class="row">';
                break;
            case 'export_xml':
                $subcontentNew = $this->exportImportXmlAction($l10NConfiguration) . '</div></div></div></div>';
                break;
            default:
                $subcontentNew = '<input class="btn btn-default" type="submit" value="' . $this->getLanguageService()->getLL('general.action.refresh.button.title') . '" name="_" />';
                break;
        }

        return $subcontentNew;
    }

    /**
     * Creating module content
     *
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    protected function moduleContent(L10nConfiguration $l10NConfiguration): void
    {
        switch ($this->MOD_SETTINGS['action'] ?? '') {
            case 'inlineEdit':
            case 'link':
                $subcontent = '';
                break;
            case 'export_excel':
                $subcontent = $this->excelExportImportAction($l10NConfiguration) . '</div></div></div></div></div><div class="row">';
                break;
            case 'export_xml':
                $subcontent = $this->exportImportXmlAction($l10NConfiguration) . '</div></div></div></div>';
                break;
            default:
                $subcontent = '<input class="btn btn-default" type="submit" value="' . $this->getLanguageService()->getLL('general.action.refresh.button.title') . '" name="_" />';
                break;
        }

        $this->content .= '<div class="col-md-6"><div class="form">' . $subcontent;
    }

    /**
     * @param L10nConfiguration $l10NConfiguration
     * @return array
     */
    protected function inlineEditAction(L10nConfiguration $l10NConfiguration): array
    {
        /** @var L10nBaseService $service */
        $service = GeneralUtility::makeInstance(L10nBaseService::class);

        // simple init of translation object:
        /** @var TranslationData $translationData */
        $translationData = GeneralUtility::makeInstance(TranslationData::class);
        $translationData->setTranslationData((array)GeneralUtility::_POST('translation'));
        $translationData->setLanguage($this->sysLanguage);
        $translationData->setPreviewLanguage($this->previewLanguage);
        // See, if incoming translation is available, if so, submit it
        if (GeneralUtility::_POST('saveInline')) {
            $service->saveTranslation($l10NConfiguration, $translationData);
        }

        // Buttons:
        $info = [];
        $info['saveConfirmation'] = 'return confirm(\'' . $this->getLanguageService()->getLL('inlineedit.save.alert.title') . '\');';
        $info['cancelConfirmation'] = 'return confirm(\'' . $this->getLanguageService()->getLL('inlineedit.cancel.alert.title') . '\');';

        return $info;
    }

    /**
     * @param L10nConfiguration $l10ncfgObj
     * @return string
     * @throws ResourceNotFoundException
     * @throws RouteNotFoundException
     */
    protected function excelExportImportAction(L10nConfiguration $l10ncfgObj): string
    {
        /** @var L10nBaseService $service */
        $service = GeneralUtility::makeInstance(L10nBaseService::class);
        if (GeneralUtility::_POST('import_asdefaultlanguage') == '1') {
            $service->setImportAsDefaultLanguage(true);
        }
        // Buttons:
        $_selectOptions = ['0' => '-default-'];
        $_selectOptions = $_selectOptions + $this->MOD_MENU['lang'];
        $info = '<div class="form-section">' .
            static::getFuncCheck(
                $this->id,
                'SET[check_exports]',
                $this->MOD_SETTINGS['check_exports'] ?? '',
                '',
                '&srcPID=' . rawurlencode(GeneralUtility::_GET('srcPID')) . '&exportUID=' . $l10ncfgObj->getId(),
                '',
                $this->getLanguageService()->getLL('export.xml.check_exports.title')
            ) .
            '<div class="form-group mb-2"><div class="checkbox"><label>' .
            '<input type="checkbox" value="1" name="import_asdefaultlanguage" /> ' . $this->getLanguageService()->getLL('import.xml.asdefaultlanguage.title') .
            '</label></div></div>' .
            '</div><div class="form-section"><div class="form-group mb-2">
<label>' . $this->getLanguageService()->getLL('export.xml.source-language.title') . '</label><br />' .
            $this->_getSelectField('export_xml_forcepreviewlanguage', (string)$this->previewLanguage, $_selectOptions) .
            '</div></div><div class="form-section"><div class="form-group mb-2">
<label>' . $this->getLanguageService()->getLL('general.action.import.upload.title') . '</label><br />' .
            '<input type="file" size="60" name="uploaded_import_file" />' .
            '</div></div><div class="form-section"><div class="form-group mb-2">' .
            '<input class="btn btn-default btn-info" type="submit" value="' . $this->getLanguageService()->getLL('general.action.refresh.button.title') . '" name="_" /> ' .
            '<input class="btn btn-default btn-success" type="submit" value="' . $this->getLanguageService()->getLL('general.action.export.xml.button.title') . '" name="export_excel" /> ' .
            '<input class="btn btn-default btn-warning" type="submit" value="' . $this->getLanguageService()->getLL('general.action.import.xml.button.title') . '" name="import_excel" />
</div></div></div>';
        // Read uploaded file:
        if (GeneralUtility::_POST('import_excel') && !empty($_FILES['uploaded_import_file']['tmp_name']) && is_uploaded_file($_FILES['uploaded_import_file']['tmp_name'])) {
            $uploadedTempFile = GeneralUtility::upload_to_tempfile($_FILES['uploaded_import_file']['tmp_name']);
            /** @var TranslationDataFactory $factory */
            $factory = GeneralUtility::makeInstance(TranslationDataFactory::class);
            //TODO: catch exeption
            $translationData = $factory->getTranslationDataFromExcelXMLFile($uploadedTempFile);
            $translationData->setLanguage($this->sysLanguage);
            $translationData->setPreviewLanguage($this->previewLanguage);
            GeneralUtility::unlink_tempfile($uploadedTempFile);
            $service->saveTranslation($l10ncfgObj, $translationData);
            $icon = $this->iconFactory->getIcon('status-dialog-notification', Icon::SIZE_SMALL)->render();
            $info .= '<br /><br />' . $icon . $this->getLanguageService()->getLL('import.success.message') . '<br /><br />';
        }
        // If export of XML is asked for, do that (this will exit and push a file for download)
        if (GeneralUtility::_POST('export_excel')) {
            // Render the XML
            /** @var ExcelXmlView $viewClass */
            $viewClass = GeneralUtility::makeInstance(ExcelXmlView::class, $l10ncfgObj, $this->sysLanguage);
            $export_xml_forcepreviewlanguage = (int)GeneralUtility::_POST('export_xml_forcepreviewlanguage');
            if ($export_xml_forcepreviewlanguage > 0) {
                $viewClass->setForcedSourceLanguage($export_xml_forcepreviewlanguage);
            }
            if ($this->MOD_SETTINGS['onlyChangedContent'] ?? false) {
                $viewClass->setModeOnlyChanged();
            }
            if ($this->MOD_SETTINGS['noHidden'] ?? false) {
                $viewClass->setModeNoHidden();
            }
            //Check the export
            if (($this->MOD_SETTINGS['check_exports'] ?? false) && !$viewClass->checkExports()) {
                /** @var FlashMessage $flashMessage */
                $flashMessage = GeneralUtility::makeInstance(
                    FlashMessage::class,
                    '###MESSAGE###',
                    $this->getLanguageService()->getLL('export.process.duplicate.title'),
                    AbstractMessage::INFO
                );
                $info .= str_replace(
                    '###MESSAGE###',
                    $this->getLanguageService()->getLL('export.process.duplicate.message'),
                    GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                        ->resolve()
                        ->render([$flashMessage])
                );
                $info .= $viewClass->renderExports();
            } else {
                try {
                    $filename = $this->downloadXML($viewClass);
                    // Prepare a success message for display
                    $link = sprintf(
                        '<a href="%s" target="_blank">%s</a>',
                        $filename,
                        $filename
                    );
                    $title = $this->getLanguageService()->getLL('export.download.success');
                    $message = '###MESSAGE###';
                    $status = AbstractMessage::OK;
                    /** @var FlashMessage $flashMessage */
                    $flashMessage = GeneralUtility::makeInstance(FlashMessage::class, $message, $title, $status);
                    $info .= str_replace(
                        '###MESSAGE###',
                        sprintf($this->getLanguageService()->getLL('export.download.success.detail'), $link),
                        GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                            ->resolve()
                            ->render([$flashMessage])
                    );
                } catch (Exception $e) {
                    // Prepare an error message for display
                    $title = $this->getLanguageService()->getLL('export.download.error');
                    $message = '###MESSAGE###';
                    $status = AbstractMessage::ERROR;
                    /** @var FlashMessage $flashMessage */
                    $flashMessage = GeneralUtility::makeInstance(FlashMessage::class, $message, $title, $status);
                    $info .= str_replace(
                        '###MESSAGE###',
                        $e->getMessage() . ' (' . $e->getCode() . ')',
                        GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                            ->resolve()
                            ->render([$flashMessage])
                    );
                }
                /** @var FlashMessage $flashMessage */
                $info .= $viewClass->renderInternalMessagesAsFlashMessage((string)$status);
                $viewClass->saveExportInformation();
            }
        }
        return $info;
    }

    /**
     * @param string $elementName
     * @param string $currentValue
     * @param array $menuItems
     * @return string
     */
    protected function _getSelectField(string $elementName, string $currentValue, array $menuItems): string
    {
        $options = [];
        $return = '';
        foreach ($menuItems as $value => $label) {
            $options[] = '<option value="' . htmlspecialchars((string)$value) . '"' . (!strcmp(
                $currentValue,
                (string)$value
            ) ? ' selected="selected"' : '') . '>' . htmlspecialchars(
                (string)$label,
                ENT_COMPAT,
                'UTF-8',
                false
            ) . '</option>';
        }
        if (count($options) > 0) {
            $return = '
	<select class="form-control" name="' . $elementName . '" ' . ($currentValue ? 'disabled="disabled"' : '') . '>
	' . implode('
	', $options) . '
	</select>
	';
        }

        if ($currentValue) {
            $return .= '<input type="hidden" name="' . $elementName . '" value="' . $currentValue . '" />';
        }

        return $return;
    }

    /**
     * Sends download header and calls render method of the view.
     * Used for excelXML and CATXML.
     *
     * @param object $xmlView Object for generating the XML export
     *
     * @return string $filename
     */
    protected function downloadXML(object $xmlView): string
    {
        // Save content to the disk and get the file name
        return $xmlView->render();
    }

    /**
     * @param L10nConfiguration $l10ncfgObj
     * @return string
     */
    protected function catXMLExportImportAction(L10nConfiguration $l10ncfgObj): string
    {
        /** @var L10nBaseService $service */
        $service = GeneralUtility::makeInstance(L10nBaseService::class);
        $menuItems = [
            '0' => [
                'label' => $this->getLanguageService()->getLL('export.xml.headline.title'),
                'content' => $this->getTabContentXmlExport(),
            ],
            '1' => [
                'label' => $this->getLanguageService()->getLL('import.xml.headline.title'),
                'content' => $this->getTabContentXmlImport(),
            ],
            '2' => [
                'label' => $this->getLanguageService()->getLL('file.settings.downloads.title'),
                'content' => $this->getTabContentXmlDownloads(),
            ],
            '3' => [
                'label' => $this->getLanguageService()->getLL('l10nmgr.documentation.title'),
                'content' => '<a class="btn btn-success" href="/' . PathUtility::stripPathSitePrefix(ExtensionManagementUtility::extPath('l10nmgr')) . 'Documentation/manual.sxw" target="_new">Download</a>',
            ],
        ];
        $info = $this->moduleTemplate->getDynamicTabMenu($menuItems, 'ddtabs');
        $actionInfo = '';
        // Read uploaded file:
        if (GeneralUtility::_POST('import_xml') && !empty($_FILES['uploaded_import_file']['tmp_name']) && is_uploaded_file($_FILES['uploaded_import_file']['tmp_name'])) {
            $uploadedTempFile = GeneralUtility::upload_to_tempfile($_FILES['uploaded_import_file']['tmp_name']);
            /** @var TranslationDataFactory $factory */
            $factory = GeneralUtility::makeInstance(TranslationDataFactory::class);
            //print "<pre>";
            //var_dump($this->getBackendUser()->user);
            //print "</pre>";
            if (GeneralUtility::_POST('import_asdefaultlanguage') == '1') {
                $service->setImportAsDefaultLanguage(true);
            }
            // Relevant processing of XML Import with the help of the Importmanager
            /** @var CatXmlImportManager $importManager */
            $importManager = GeneralUtility::makeInstance(
                CatXmlImportManager::class,
                $uploadedTempFile,
                $this->sysLanguage,
                $xmlString = ''
            );
            if ($importManager->parseAndCheckXMLFile() === false) {
                $actionInfo .= '<br /><br />' . $this->moduleTemplate->header($this->getLanguageService()->getLL('import.error.title')) . $importManager->getErrorMessages();
            } else {
                if (GeneralUtility::_POST('import_delL10N') == '1') {
                    $actionInfo .= $this->getLanguageService()->getLL('import.xml.delL10N.message') . '<br />';
                    $delCount = $importManager->delL10N($importManager->getDelL10NDataFromCATXMLNodes($importManager->getXMLNodes()));
                    $actionInfo .= sprintf(
                        $this->getLanguageService()->getLL('import.xml.delL10N.count.message'),
                        $delCount
                    ) . '<br /><br />';
                }
                if (GeneralUtility::_POST('make_preview_link') == '1' && ExtensionManagementUtility::isLoaded('workspaces')) {
                    $pageIds = $importManager->getPidsFromCATXMLNodes($importManager->getXMLNodes());
                    $actionInfo .= '<b>' . $this->getLanguageService()->getLL('import.xml.preview_links.title') . '</b><br />';
                    /** @var MkPreviewLinkService $mkPreviewLinks */
                    $mkPreviewLinks = GeneralUtility::makeInstance(
                        MkPreviewLinkService::class,
                        $importManager->headerData['t3_workspaceId'] ?? 0,
                        $importManager->headerData['t3_sysLang'] ?? 0,
                        $pageIds
                    );
                    $actionInfo .= $mkPreviewLinks->renderPreviewLinks($mkPreviewLinks->mkPreviewLinks());
                }
                if (!empty($importManager->headerData['t3_sourceLang']) && !empty($importManager->headerData['t3_targetLang'])
                    && $importManager->headerData['t3_sourceLang'] === $importManager->headerData['t3_targetLang']) {
                    $this->previewLanguage = $this->sysLanguage;
                }
                $translationData = $factory->getTranslationDataFromCATXMLNodes($importManager->getXMLNodes());
                $translationData->setLanguage($this->sysLanguage);
                $translationData->setPreviewLanguage($this->previewLanguage);
                //$actionInfo.="<pre>".var_export($GLOBALS['BE_USER'],true)."</pre>";
                unset($importManager);
                $service->saveTranslation($l10ncfgObj, $translationData);
                $icon = $this->iconFactory->getIcon('status-dialog-notification', Icon::SIZE_SMALL)->render();
                $actionInfo .= '<br /><br />' . $icon . 'Import done<br /><br />(Command count:' . $service->lastTCEMAINCommandsCount . ')';
            }
            GeneralUtility::unlink_tempfile($uploadedTempFile);
        }
        // If export of XML is asked for, do that (this will exit and push a file for download, or upload to FTP is option is checked)
        if (GeneralUtility::_POST('export_xml')) {
            // Save user prefs
            $this->getBackendUser()->pushModuleData('l10nmgr/cm1/checkUTF8', GeneralUtility::_POST('check_utf8'));
            // Render the XML
            /** @var CatXmlView $viewClass */
            $viewClass = GeneralUtility::makeInstance(CatXmlView::class, $l10ncfgObj, $this->sysLanguage);
            $export_xml_forcepreviewlanguage = (int)GeneralUtility::_POST('export_xml_forcepreviewlanguage');
            if ($export_xml_forcepreviewlanguage > 0) {
                $viewClass->setForcedSourceLanguage($export_xml_forcepreviewlanguage);
            }
            if ($this->MOD_SETTINGS['onlyChangedContent'] ?? false) {
                $viewClass->setModeOnlyChanged();
            }
            if ($this->MOD_SETTINGS['noHidden'] ?? false) {
                $viewClass->setModeNoHidden();
            }
            // Check the export
            if (($this->MOD_SETTINGS['check_exports'] ?? false) && !$viewClass->checkExports()) {
                /** @var FlashMessage $flashMessage */
                $flashMessage = GeneralUtility::makeInstance(
                    FlashMessage::class,
                    '###MESSAGE###',
                    $this->getLanguageService()->getLL('export.process.duplicate.title'),
                    AbstractMessage::INFO
                );
                $actionInfo .= str_replace(
                    '###MESSAGE###',
                    $this->getLanguageService()->getLL('export.process.duplicate.message'),
                    GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                        ->resolve()
                        ->render([$flashMessage])
                );
                $actionInfo .= $viewClass->renderExports();
            } else {
                // Upload to FTP
                if (GeneralUtility::_POST('ftp_upload') == '1') {
                    try {
                        $filename = $this->uploadToFtp($viewClass);
                        // Send a mail notification
                        $this->emailNotification($filename, $l10ncfgObj, $this->sysLanguage);
                        // Prepare a success message for display
                        $title = $this->getLanguageService()->getLL('export.ftp.success');
                        $message = '###MESSAGE###';
                        $status = AbstractMessage::OK;
                        /** @var FlashMessage $flashMessage */
                        $flashMessage = GeneralUtility::makeInstance(FlashMessage::class, $message, $title, $status);
                        $actionInfo .= str_replace(
                            '###MESSAGE###',
                            sprintf(
                                $this->getLanguageService()->getLL('export.ftp.success.detail'),
                                $this->emConfiguration->getFtpServerPath() . $filename
                            ),
                            GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                                ->resolve()
                                ->render([$flashMessage])
                        );
                    } catch (Exception $e) {
                        // Prepare an error message for display
                        $title = $this->getLanguageService()->getLL('export.ftp.error');
                        $message = '###MESSAGE###';
                        $status = AbstractMessage::ERROR;
                        /** @var FlashMessage $flashMessage */
                        $flashMessage = GeneralUtility::makeInstance(FlashMessage::class, $message, $title, $status);
                        $actionInfo .= str_replace(
                            '###MESSAGE###',
                            $e->getMessage() . ' (' . $e->getCode() . ')',
                            GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                                ->resolve()
                                ->render([$flashMessage])
                        );
                    }
                    // Download the XML file
                } else {
                    try {
                        $filename = $this->downloadXML($viewClass);
                        // Prepare a success message for display
                        $link = sprintf(
                            '<a href="%s" target="_blank">%s</a>',
                            $filename,
                            $filename
                        );
                        $title = $this->getLanguageService()->getLL('export.download.success');
                        $message = '###MESSAGE###';
                        $status = AbstractMessage::OK;
                        /** @var FlashMessage $flashMessage */
                        $flashMessage = GeneralUtility::makeInstance(FlashMessage::class, $message, $title, $status);
                        $actionInfo .= str_replace(
                            '###MESSAGE###',
                            sprintf($this->getLanguageService()->getLL('export.download.success.detail'), $link),
                            GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                                ->resolve()
                                ->render([$flashMessage])
                        );
                    } catch (Exception $e) {
                        // Prepare an error message for display
                        $title = $this->getLanguageService()->getLL('export.download.error');
                        $message = '###MESSAGE###';
                        $status = AbstractMessage::ERROR;
                        /** @var FlashMessage $flashMessage */
                        $flashMessage = GeneralUtility::makeInstance(FlashMessage::class, $message, $title, $status);
                        $actionInfo .= str_replace(
                            '###MESSAGE###',
                            $e->getMessage() . ' (' . $e->getCode() . ')',
                            GeneralUtility::makeInstance(FlashMessageRendererResolver::class)
                                ->resolve()
                                ->render([$flashMessage])
                        );
                    }
                }
                /** @var $flashMessage FlashMessage */
                $actionInfo .= $viewClass->renderInternalMessagesAsFlashMessage((string)$status);
                $viewClass->saveExportInformation();
            }
        }
        if (!empty($actionInfo)) {
            $info .= $this->moduleTemplate->header($this->getLanguageService()->getLL('misc.messages.title'));
            $info .= $actionInfo;
        }
        // $info .= '</div>';
        return $info;
    }

    /**
     * @return string
     */
    protected function getTabContentXmlExport(): string
    {
        $_selectOptions = ['0' => '-default-'];
        $_selectOptions = $_selectOptions + ($this->MOD_MENU['lang'] ?? []);
        $tabContentXmlExport = '<div class="form-section">' .
            '<div class="form-group mb-2"><div class="checkbox"><label>' .
            '<input type="checkbox" value="1" name="check_exports" /> ' . $this->getLanguageService()->getLL('export.xml.check_exports.title') .
            '</label></div></div>' .
            '<div class="form-group mb-2"><div class="checkbox"><label>' .
            '<input type="checkbox" value="1" checked="checked" name="no_check_xml" /> ' . $this->getLanguageService()->getLL('export.xml.no_check_xml.title') .
            '</label></div></div>' .
            '<div class="form-group mb-2"><div class="checkbox"><label>' .
            '<input type="checkbox" value="1" name="check_utf8" /> ' . $this->getLanguageService()->getLL('export.xml.checkUtf8.title') .
            '</label></div></div>' .
            '</div><div class="form-section">' .
            '<div class="form-group mb-2">' .
            '<label>' . $this->getLanguageService()->getLL('export.xml.source-language.title') . '</label><br />' .
            $this->_getSelectField('export_xml_forcepreviewlanguage', (string)$this->previewLanguage, $_selectOptions) .
            '</div></div>';
        // Add the option to send to FTP server, if FTP information is defined
        if ($this->emConfiguration->hasFtpCredentials()) {
            $tabContentXmlExport .= '<input type="checkbox" value="1" name="ftp_upload" id="tx_l10nmgr_ftp_upload" />
<label for="tx_l10nmgr_ftp_upload">' . $this->getLanguageService()->getLL('export.xml.ftp.title') . '</label><br />';
        }
        $tabContentXmlExport .= '<div class="form-section"><input class="btn btn-default btn-info" type="submit" value="' . $this->getLanguageService()->getLL('general.action.refresh.button.title') . '" name="_" /> ' .
            '<input class="btn btn-default btn-success" type="submit" value="Export" name="export_xml" /><br class="clearfix">&nbsp;</div>';
        return $tabContentXmlExport;
    }

    /**
     * @return string
     */
    protected function getTabContentXmlImport(): string
    {
        return '<div class="form-section">' .
            (
                ExtensionManagementUtility::isLoaded('workspaces') ? (
                    '<div class="form-group mb-2"><div class="checkbox"><label>' .
                '<input type="checkbox" value="1" name="make_preview_link" /> ' . $this->getLanguageService()->getLL('import.xml.make_preview_link.title') .
                '</label></div></div>'
                ) : ''
            ) .
            '<div class="form-group mb-2"><div class="checkbox"><label>' .
            '<input type="checkbox" value="1" name="import_delL10N" /> ' . $this->getLanguageService()->getLL('import.xml.delL10N.title') .
            '</label></div></div>' .
            '<div class="form-group mb-2"><div class="checkbox"><label>' .
            '<input type="checkbox" value="1" name="import_asdefaultlanguage" /> ' . $this->getLanguageService()->getLL('import.xml.asdefaultlanguage.title') .
            '</label></div></div></div>' .
            '<div class="form-section"><div class="form-group mb-2">' .
            '<input type="file" size="60" name="uploaded_import_file" />' .
            '</div></div>' .
            '<div class="form-section">' .
            '<input class="btn btn-info" type="submit" value="' . $this->getLanguageService()->getLL('general.action.refresh.button.title') . '" name="_" /> ' .
            '<input class="btn btn-warning" type="submit" value="Import" name="import_xml" />' .
            '<br class="clearfix">&nbsp;</div>';
    }

    /**
     * @throws RouteNotFoundException
     */
    /**
     * @return string
     * @throws RouteNotFoundException
     */
    protected function getTabContentXmlDownloads(): string
    {
        $tabContentXmlDownloads = '<h4>' . $this->getLanguageService()->getLL('file.settings.available.title') . '</h4><ul>';
        $uriBuilder = GeneralUtility::makeInstance(UriBuilder::class);
        foreach ($this->settings as $settingId => $settingFileName) {
            $absoluteFileName = GeneralUtility::getFileAbsFileName('EXT:l10nmgr/Configuration/Settings/' . $settingFileName);
            if (is_file($absoluteFileName) && is_readable($absoluteFileName)) {
                $size = GeneralUtility::formatSize((int)filesize($absoluteFileName), ' Bytes| KB| MB| GB');
                $tabContentXmlDownloads .= '<li><a class="t3-link" href="' . $uriBuilder->buildUriFromRoute('download_setting', ['setting' => $settingId]) . '" title="' . $this->getLanguageService()->getLL('file.settings.download.title') . '" target="_blank">' . $this->getLanguageService()->getLL('file.settings.' . $settingId . '.title') . ' (' . $size . ')' . '</a></li>';
            }
        }
        $tabContentXmlDownloads .= '</ul>';
        return $tabContentXmlDownloads;
    }

    /**
     * @param ServerRequestInterface $request
     * @return ResponseInterface
     */
    public function downloadSetting(ServerRequestInterface $request): ResponseInterface
    {
        $settingId = $request->getQueryParams()['setting'];
        $absoluteFileName = GeneralUtility::getFileAbsFileName('EXT:l10nmgr/Configuration/Settings/' . $this->getSetting($settingId));

        $body = new Stream('php://temp', 'wb+');
        $body->write(file_get_contents($absoluteFileName));
        $body->rewind();
        return (new Response())
            ->withAddedHeader('Content-Length', (string)(filesize($absoluteFileName) ?: ''))
            ->withAddedHeader('Content-Disposition', 'attachment; filename="' . PathUtility::basename($absoluteFileName) . '"')
            ->withBody($body);
    }

    /**
     * @param string $key
     * @return string
     */
    protected function getSetting(string $key): string
    {
        return $this->settings[$key] ?? '';
    }

    /**
     * Uploads the XML export to the FTP server
     *
     * @param CatXmlView $xmlView Object for generating the XML export
     *
     * @return string The file name, if successful
     * @throws Exception
     */
    protected function uploadToFtp(CatXmlView $xmlView): string
    {
        // Save content to the disk and get the file name
        $filename = $xmlView->render();
        $xmlFileName = basename($filename);
        // Try connecting to FTP server and uploading the file
        // If any step fails, an exception is thrown
        $connection = ftp_connect($this->emConfiguration->getFtpServer());
        if ($connection) {
            if (@ftp_login(
                $connection,
                $this->emConfiguration->getFtpServerUsername(),
                $this->emConfiguration->getFtpServerPassword()
            )) {
                if (ftp_put(
                    $connection,
                    $this->emConfiguration->getFtpServerPath() . $xmlFileName,
                    Environment::getPublicPath() . '/' . $filename,
                    FTP_BINARY
                )) {
                    ftp_close($connection);
                } else {
                    ftp_close($connection);
                    throw new Exception(sprintf(
                        $this->getLanguageService()->getLL('export.ftp.upload_failed'),
                        $filename,
                        $this->emConfiguration->getFtpServerPath()
                    ), 1326906926);
                }
            } else {
                ftp_close($connection);
                throw new Exception(sprintf(
                    $this->getLanguageService()->getLL('export.ftp.login_failed'),
                    $this->emConfiguration->getFtpServerUsername()
                ), 1326906772);
            }
        } else {
            throw new Exception($this->getLanguageService()->getLL('export.ftp.connection_failed'), 1326906675);
        }
        // If everything went well, return the file's base name
        return $xmlFileName;
    }

    /**
     * The function emailNotification sends an email with a translation job to the recipient specified in the extension config.
     *
     * @param string $xmlFileName Name of the XML file
     * @param L10nConfiguration $l10nmgrCfgObj L10N Manager configuration object
     * @param int $tlang ID of the language to translate to
     */
    protected function emailNotification(string $xmlFileName, L10nConfiguration $l10nmgrCfgObj, int $tlang)
    {
        // If at least a recipient is indeed defined, proceed with sending the mail
        $recipients = GeneralUtility::trimExplode(',', $this->emConfiguration->getEmailRecipient());
        if (count($recipients) > 0) {
            $fullFilename = Environment::getPublicPath() . '/uploads/tx_l10nmgr/jobs/out/' . $xmlFileName;
            // Get source & target language ISO codes
            $sourceStaticLangArr = BackendUtility::getRecord(
                'static_languages',
                $l10nmgrCfgObj->l10ncfg['sourceLangStaticId'] ?? 0,
                'lg_iso_2'
            );
            $targetStaticLang = BackendUtility::getRecord('sys_language', $tlang, 'static_lang_isocode');
            $targetStaticLangArr = BackendUtility::getRecord(
                'static_languages',
                $targetStaticLang['static_lang_isocode'] ?? 0,
                'lg_iso_2'
            );
            $sourceLang = $sourceStaticLangArr['lg_iso_2'] ?? 0;
            $targetLang = $targetStaticLangArr['lg_iso_2'] ?? 0;
            // Collect mail data
            $fromMail = $this->emConfiguration->getEmailSender();
            $fromName = $this->emConfiguration->getEmailSenderName();
            $subject = sprintf(
                $this->getLanguageService()->getLL('email.suject.msg'),
                $sourceLang,
                $targetLang,
                $GLOBALS['TYPO3_CONF_VARS']['SYS']['sitename'] ?? ''
            );
            // Assemble message body
            $message = [
                'msg1' => $this->getLanguageService()->getLL('email.greeting.msg'),
                'msg2' => '',
                'msg3' => sprintf(
                    $this->getLanguageService()->getLL('email.new_translation_job.msg'),
                    $sourceLang,
                    $targetLang,
                    $GLOBALS['TYPO3_CONF_VARS']['SYS']['sitename'] ?? ''
                ),
                'msg4' => $this->getLanguageService()->getLL('email.info.msg'),
                'msg5' => $this->getLanguageService()->getLL('email.info.import.msg'),
                'msg6' => '',
                'msg7' => $this->getLanguageService()->getLL('email.goodbye.msg'),
                'msg8' => $fromName,
                'msg9' => '--',
                'msg10' => $this->getLanguageService()->getLL('email.info.exported_file.msg'),
                'msg11' => $xmlFileName,
            ];
            if ($this->emConfiguration->isEmailAttachment()) {
                $message['msg3'] = sprintf(
                    $this->getLanguageService()->getLL('email.new_translation_job_attached.msg'),
                    $sourceLang,
                    $targetLang,
                    $GLOBALS['TYPO3_CONF_VARS']['SYS']['sitename'] ?? ''
                );
            }
            $msg = implode(chr(10), $message);
            // Instantiate the mail object, set all necessary properties and send the mail
            /** @var MailMessage $mailObject */
            $mailObject = GeneralUtility::makeInstance(MailMessage::class);
            $mailObject->setFrom([$fromMail => $fromName]);
            $mailObject->setTo($recipients);
            $mailObject->setSubject($subject);
            $mailObject->text($msg);
            if ($this->emConfiguration->isEmailAttachment()) {
                $mailObject->attachFromPath($fullFilename);
            }
            $mailObject->send();
        }
    }

    /**
     * Adds items to the ->MOD_MENU array. Used for the function menu selector.
     */
    public function menuConfig(): void
    {
        $this->MOD_MENU = [
            'action' => [
                '' => $this->getLanguageService()->getLL('general.action.blank.title'),
                'link' => $this->getLanguageService()->getLL('general.action.edit.link.title'),
                'inlineEdit' => $this->getLanguageService()->getLL('general.action.edit.inline.title'),
                'export_excel' => $this->getLanguageService()->getLL('general.action.export.excel.title'),
                'export_xml' => $this->getLanguageService()->getLL('general.action.export.xml.title'),
            ],
            'lang' => [],
            'onlyChangedContent' => '',
            'check_exports' => 1,
            'noHidden' => '',
        ];

        $configurationId = (int)($GLOBALS['TYPO3_REQUEST']->getQueryParams()['exportUID'] ?? 0);
        $configuration = BackendUtility::getRecord('tx_l10nmgr_cfg', $configurationId);
        $targetLanguages = [];
        if (!empty($configuration['targetLanguages'])) {
            $targetLanguages = array_flip(GeneralUtility::intExplode(',', $configuration['targetLanguages'], true));
        }

        // TODO: Migrate to SiteConfiguration
        // Load system languages into menu and check against allowed languages:
        /** @var TranslationConfigurationProvider $t8Tools */
        $t8Tools = GeneralUtility::makeInstance(TranslationConfigurationProvider::class);
        $sysL = $t8Tools->getSystemLanguages();
        foreach ($sysL as $sL) {
            if (!empty($targetLanguages) && !isset($targetLanguages[$sL['uid']])) {
                continue;
            }
            if (empty($this->MOD_MENU['lang'])) {
                $this->MOD_MENU['lang'] = [];
            }
            if ($sL['uid'] > 0 && $this->getBackendUser()->checkLanguageAccess($sL['uid'])) {
                if ($this->emConfiguration->isEnableHiddenLanguages()) {
                    $this->MOD_MENU['lang'][$sL['uid']] = $sL['title'];
                } elseif (!($sL['hidden'] ?? false)) {
                    $this->MOD_MENU['lang'][$sL['uid']] = $sL['title'];
                }
            }
        }
        parent::menuConfig();
    }

    /**
     * @return StandaloneView
     */
    protected function getFluidTemplateObject(): StandaloneView
    {
        $view = GeneralUtility::makeInstance(StandaloneView::class);
        $view->setLayoutRootPaths([GeneralUtility::getFileAbsFileName('EXT:l10nmgr/Resources/Private/Layouts')]);
        $view->setPartialRootPaths([GeneralUtility::getFileAbsFileName('EXT:l10nmgr/Resources/Private/Partials')]);
        $view->setTemplateRootPaths([GeneralUtility::getFileAbsFileName('EXT:l10nmgr/Resources/Private/Templates')]);

        $view->setTemplatePathAndFilename(GeneralUtility::getFileAbsFileName('EXT:l10nmgr/Resources/Private/Templates/LocalizationManager/Index.html'));

        $view->getRequest()->setControllerExtensionName('l10nmgr');

        return $view;
    }

    /**
     * @return L10nConfiguration
     */
    protected function getL10NConfiguration(): L10nConfiguration
    {
        /** @var L10nConfiguration $l10nConfiguration */
        $l10nConfiguration = GeneralUtility::makeInstance(L10nConfiguration::class);
        $l10nConfiguration->load((int)GeneralUtility::_GP('exportUID'));

        return $l10nConfiguration;
    }

    /**
     * @param L10nConfiguration $l10NConfiguration
     * @param array $result
     * @return array
     */
    protected function linkOverviewAndOnlineTranslationAction(
        L10nConfiguration $l10NConfiguration,
        array $result
    ): array {
        /** @var L10nHTMLListView $htmlListView */
        $htmlListView = GeneralUtility::makeInstance(
            L10nHtmlListView::class,
            $l10NConfiguration,
            $this->sysLanguage
        );
        $action = $this->MOD_SETTINGS['action'] ?? '';
        // Render the module content (for all modes):
        if ($action === 'inlineEdit') {
            $result['inlineEdit'] = $this->inlineEditAction($l10NConfiguration);
            $htmlListView->setModeWithInlineEdit();
        }
        //*******************************************
        if ($this->MOD_SETTINGS['onlyChangedContent'] ?? false) {
            $htmlListView->setModeOnlyChanged();
        }
        if ($this->MOD_SETTINGS['noHidden'] ?? false) {
            $htmlListView->setModeNoHidden();
        }
        if ($action === 'link') {
            $htmlListView->setModeShowEditLinks();
        }

        return [
            'sections' => $htmlListView->renderOverview(),
            'inlineEdit' => $result['inlineEdit'] ?? [],
        ];
    }

    /**
     * @param L10nConfiguration $l10NConfiguration
     * @return string
     */
    protected function exportImportXmlAction(L10nConfiguration $l10NConfiguration): string
    {
        $prefs['utf8'] = GeneralUtility::_POST('check_utf8');
        $prefs['noxmlcheck'] = GeneralUtility::_POST('no_check_xml');
        $prefs['check_exports'] = GeneralUtility::_POST('check_exports');
        $this->getBackendUser()->pushModuleData('l10nmgr/cm1/prefs', $prefs);

        return $this->catXMLExportImportAction($l10NConfiguration);
    }

    /**
     * @param L10nConfiguration $l10nConfiguration
     * @return array
     */
    protected function renderConfigurationTable(L10nConfiguration $l10nConfiguration): array
    {
        /** @var L10nConfigurationDetailView $l10nmgrconfigurationView */
        $l10nmgrconfigurationView = GeneralUtility::makeInstance(
            L10nConfigurationDetailView::class,
            $l10nConfiguration
        );

        return $l10nmgrconfigurationView->render();
    }
}
