<?php
if (!defined('TYPO3_MODE')) {
    die('Access denied.');
}

\TYPO3\CMS\Core\Utility\ExtensionManagementUtility::addUserTSConfig('
	options.saveDocNew.tx_l10nmgr_cfg=1
	options.saveDocNew.tx_l10nmgr_priorities=1
');

//! increase with every change to XML Format
const L10NMGR_FILEVERSION = '2.0';
const L10NMGR_VERSION = '11.0.0';
$GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['t3lib/class.t3lib_tcemain.php']['processDatamapClass']['tx_l10nmgr'] = \Localizationteam\L10nmgr\Hooks\Tcemain::class;

// Enable stats
$enableStatHook = \TYPO3\CMS\Core\Utility\GeneralUtility::makeInstance(
    \TYPO3\CMS\Core\Configuration\ExtensionConfiguration::class
)->get('l10nmgr', 'enable_stat_hook');
if ($enableStatHook) {
    $GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['GLOBAL']['recStatInfoHooks']['tx_l10nmgr'] = \Localizationteam\L10nmgr\Hooks\Tcemain::class . '->stat';
}

// Add file cleanup task
$GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['scheduler']['tasks'][\Localizationteam\L10nmgr\Task\L10nmgrFileGarbageCollection::class] = [
    'extension'        => 'l10nmgr',
    'title'            => 'LLL:EXT:l10nmgr/Resources/Private/Language/Task/locallang.xlf:fileGarbageCollection.name',
    'description'      => 'LLL:EXT:l10nmgr/Resources/Private/Language/Task/locallang.xlf:fileGarbageCollection.description',
    'additionalFields' => \Localizationteam\L10nmgr\Task\L10nmgrAdditionalFieldProvider::class,
];

\TYPO3\CMS\Core\Utility\ExtensionManagementUtility::addPageTSConfig(
    '@import \'EXT:l10nmgr/Configuration/TSConfig/PageTSConfig.typoscript\''
);

$GLOBALS['TYPO3_CONF_VARS']['FE']['addRootLineFields'] .= ',l10nmgr_configuration,l10nmgr_configuration_next_level';
