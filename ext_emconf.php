<?php
/***************************************************************
 * Extension Manager/Repository config file for ext "l10nmgr".
 * Auto generated 10-03-2015 18:54
 * Manual updates:
 * Only the data in the array - everything else is removed by next
 * writing. "version" and "dependencies" must not be touched!
 ***************************************************************/
$EM_CONF[$_EXTKEY] = [
    'title'            => 'Localization Manager',
    'description'      => 'Module for managing localization import and export',
    'category'         => 'module',
    'version'          => '11.0.0',
    'state'            => 'stable',
    'uploadfolder'     => false,
    // TODO: The option `createDirs` is not supported in v10 anymore and should be removed. If the extension needs folders, they must created via `GeneralUtility::mkdir_deep()`.
    // see: https://docs.typo3.org/c/typo3/cms-core/master/en-us/Changelog/10.0/Breaking-88525-RemoveCreateDirsDirectiveOfExtensionInstallationEm_confphp.html
    'createDirs'       => 'uploads/tx_l10nmgr/settings,uploads/tx_l10nmgr/saved_files,uploads/tx_l10nmgr/jobs,uploads/tx_l10nmgr/jobs/in,uploads/tx_l10nmgr/jobs/done,uploads/tx_l10nmgr/jobs/_cmd',
    'clearCacheOnLoad' => true,
    'author'           => 'Kasper Skaarhoej, Daniel Zielinski, Daniel Poetzinger, Fabian Seltmann, Andreas Otto, Jo Hasenau, Peter Russ',
    'author_email'     => 'kasperYYYY@typo3.com, info@loctimize.com, info@cybercraft.de, pruss@uon.li',
    'author_company'   => 'Localization Manager Team',
    'constraints'      => [
        'depends'   => [
            'typo3'              => '10.0.0-11.5.99',
            'scheduler'          => '10.0.0-11.5.99',
        ],
        'conflicts' => [],
        'suggests'  => [],
    ],
];
