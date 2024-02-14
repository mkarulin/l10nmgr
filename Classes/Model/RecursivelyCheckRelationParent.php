<?php

declare(strict_types=1);

namespace Localizationteam\L10nmgr\Model;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception;
use function is_array;
use PDO;
use TYPO3\CMS\Backend\Utility\BackendUtility;
use TYPO3\CMS\Core\Authentication\BackendUserAuthentication;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Database\Query\Restriction\DeletedRestriction;
use TYPO3\CMS\Core\Database\Query\Restriction\WorkspaceRestriction;
use TYPO3\CMS\Core\DataHandling\DataHandler;
use TYPO3\CMS\Core\Utility\GeneralUtility;

class RecursivelyCheckRelationParent
{
    protected int $depthCounter = 0;

    /**
     * The records that have already been checked.
     *
     * Has the following format: `$this->checkedRecords[table name][record uid] = true`.
     *
     * @var array
     */
    protected array $checkedRecords = [];

    /**
     * Configuration for a single inline relation where the parent table is tt_content.
     *
     * This configuration has the following format:
     *
     * $GLOBALS['TYPO3_CONF_VARS']['EXTCONF']['l10nmgr']['inlineTablesConfig'] = [
     *  'table name' => [
     *      // The field that contains the foreign_key of the parent.
     *      // This field is member of the child table.
     *      'parentField' => 'tt_content',
     *      // The field that contains the children.
     *      // This field is member of the parent table.
     *      'childrenField' => 'tx_ext_relation_table'
     *  ],
     * ];
     *
     * The parentField must have a TCA field configuration of type select with renderType selectSingle
     * and a set value for foreign_table.
     *
     * The childrenField must have a TCA field configuration of type inline with foreign_table set and
     * foreign_table_field not set.
     *
     * @var array|mixed
     */
    protected array $inlineTablesConfig;

    /**
     * Configuration for additional inline relations. This configuration allows multiple relations for any
     * child-parent-table combinations.
     *
     * This configuration has the following format:
     *
     * $GLOBALS['TYPO3_CONF_VARS']['EXTCONF']['l10nmgr']['additionalInlineTablesConfig'] = [
     *  'child table' => [[
     *      // The name of the parent table.
     *      'parentTable' => 'not_tt_content',
     *      // The field that contains the foreign_key of the parent.
     *      // This field is member of the child table.
     *      'parentField' => 'tt_content',
     *      // If the relation has a children field on the parent table,
     *      // the childrenField property must be set to this fields name.
     *      // Otherwise, if this relation can only be viewed from the child-side
     *      // and no such field exists, this property should not be set.
     *      // If there's no childrenField, there must be a data handler hook in place
     *      // that localizes all child record when a parent record gets localized,
     *      // like B13\Container\Hooks\Datahandler\CommandMapPostProcessingHook::processCmdmap_postProcess.
     *      'childrenField' => 'tx_ext_relation_table'
     *  ]],
     * ];
     *
     * The parentField must have a TCA field configuration of type select with renderType selectSingle
     * and a set value for foreign_table.
     *
     * If the childrenField property is set, it must have a TCA field configuration of type inline with foreign_table
     * set and foreign_table_field not set.
     *
     * @var array
     */
    protected array $additionalInlineTablesConfig;

    public function __construct()
    {
        $this->inlineTablesConfig = $GLOBALS['TYPO3_CONF_VARS']['EXTCONF']['l10nmgr']['inlineTablesConfig'] ?? [];
        $this->additionalInlineTablesConfig =
            $GLOBALS['TYPO3_CONF_VARS']['EXTCONF']['l10nmgr']['additionalInlineTablesConfig'] ?? [];
    }

    /**
     * Given a typo3 record, make sure that all necessary parent records are localized before localizing the
     * record itself.
     *
     * @param array $defaultLanguageChildElement the child element record in the default language.
     * It must have a uid column of type int.
     * @param int $targetLanguage the system language uid of the target language.
     * @param string $tableName the table name of the child element
     * @param array $dataHandlerCommands a reference to the {@link DataHandler} command array
     * where the commands, necessary to localize the given record, will be put into.
     * @param array $implicitlyTranslatedRecords contains the records, that will be translated after executing the
     * commands in the data handler command array, but that won't get an entry in the
     * {@link DataHandler} array of the data handler that was used
     * to execute them.
     * Has the format {@code $implicitlyTranslatedRecords[table name][default language record id] = true}.
     * @throws DBALException
     * @throws Exception
     * @throws \Doctrine\DBAL\Driver\Exception
     */
    public function localizeRecordAndRequiredParents(
        array $defaultLanguageChildElement,
        int $targetLanguage,
        string $tableName,
        array &$dataHandlerCommands,
        array &$implicitlyTranslatedRecords
    ): void {
        $this->depthCounter = 0;
        $this->localizeRecordAndRequireParentsRecursively(
            $defaultLanguageChildElement,
            $targetLanguage,
            $tableName,
            $dataHandlerCommands,
            $implicitlyTranslatedRecords
        );
    }

    /**
     * Given a typo3 child record, make sure that all necessary parent records are localized before localizing the
     * child record itself.
     *
     * This is the actual recursive function that increments the depth counter.
     *
     * @param array $element the child element record in the default language.
     * It must have a uid column of type int.
     * @param int $targetLanguage the system language uid of the target language.
     * @param string $tableName the table name of the child element
     * @param array $dataHandlerCommands a reference to the {@link DataHandler} command array
     * where the commands, necessary to localize the given record, will be put into.
     * @param array $implicitlyTranslatedRecords contains the records, that will be translated after executing the
     * commands in the data handler command array, but that won't have an entry in the
     * \TYPO3\CMS\Core\DataHandling\DataHandler::$copyMappingArray_merged array the the data handler, that was used
     * to execute them. Has the format `$implicitlyTranslatedRecords[table name][default language record id] = true`
     * @throws DBALException
     * @throws Exception
     * @throws \Doctrine\DBAL\Driver\Exception
     */
    protected function localizeRecordAndRequireParentsRecursively(
        array $element,
        int $targetLanguage,
        string $tableName,
        array &$dataHandlerCommands,
        array &$implicitlyTranslatedRecords
    ): void {
        $this->depthCounter++;

        if ($this->depthCounter >= 100) {
            return;
        }

        // we cannot process records with uid column.
        if (!array_key_exists('uid', $element)) {
            return;
        }

        $uniqueElementId = (int)$element['uid'];

        if (isset($this->checkedRecords[$tableName][$uniqueElementId])) {
            return;
        }

        $this->checkedRecords[$tableName][$uniqueElementId] = true;

        $translationParentPointerField = $this->getTranslationParentPointerField($tableName);
        $tableLanguageField = $this->getLanguageField($tableName);

        // Table has either no language field or no parent translation pointer field
        // and thus cannot be localized.
        if ($translationParentPointerField === null || $tableLanguageField === null) {
            return;
        }

        /**
         * A command, which causes the localization of the given default language element, has been put into the
         * data handler command array reference.
         */
        $elementGotLocalized = false;

        // for child tables with tt_content as parent table
        if (
            array_key_exists($tableName, $this->inlineTablesConfig)
            && is_array($this->inlineTablesConfig[$tableName])
        ) {
            $configuration = $this->inlineTablesConfig[$tableName];
            $parentField = (string)($configuration['parentField'] ?? '');
            $childrenField = (string)($configuration['childrenField'] ?? '');

            $elementGotLocalized = $this->localizeRecordFromParentSideOfRelation(
                $element,
                $targetLanguage,
                'tt_content',
                $parentField,
                $childrenField,
                $dataHandlerCommands,
                $implicitlyTranslatedRecords
            );
        }

        // for all the other relations
        if (
            array_key_exists($tableName, $this->additionalInlineTablesConfig)
            && is_array($this->additionalInlineTablesConfig[$tableName])
        ) {
            foreach ($this->additionalInlineTablesConfig[$tableName] as $additionalConfig) {
                $parentTable = (string)($additionalConfig['parentTable'] ?? '');
                $parentField = (string)($additionalConfig['parentField'] ?? '');
                $childrenField = array_key_exists('childrenField', $additionalConfig)
                    ? (string)($additionalConfig['childrenField'])
                    : null;

                if ($this->localizeRecordFromParentSideOfRelation(
                    $element,
                    $targetLanguage,
                    $parentTable,
                    $parentField,
                    $childrenField,
                    $dataHandlerCommands,
                    $implicitlyTranslatedRecords
                )) {
                    $elementGotLocalized = true;
                }
            }
        }

        if (!$elementGotLocalized) {
            $dataHandlerCommands[$tableName][$uniqueElementId]['localize'] = $targetLanguage;
        } else {
            $implicitlyTranslatedRecords[$tableName][$uniqueElementId] = true;
        }
    }

    /**
     * Fetch the column name that contains the pointer to the table's translation parent (sys_language_uid = 0 record)
     * from $GLOBALS['TCA'}. This field is called `l10n_parent` by convention.
     *
     * @link https://docs.typo3.org/m/typo3/reference-tca/main/en-us/Ctrl/Properties/TransOrigPointerField.html
     * @param string $tableName
     * @return string|null the name of the column or null, if not configured = table does not support localizations.
     */
    protected function getTranslationParentPointerField(string $tableName): ?string
    {
        if (empty($GLOBALS['TCA'][$tableName]['ctrl']['transOrigPointerField'] ?? [])) {
            return null;
        }

        return (string)$GLOBALS['TCA'][$tableName]['ctrl']['transOrigPointerField'];
    }

    /**
     * Fetch the column name that contains the language uid. This field is called `sys_language_uid` by convention.
     *
     * @link https://docs.typo3.org/m/typo3/reference-tca/main/en-us/Ctrl/Properties/LanguageField.html
     * @param string $tableName
     * @return string|null the name of the column or null, if not configured = table does not support localizations.
     */
    protected function getLanguageField(string $tableName): ?string
    {
        if (empty($GLOBALS['TCA'][$tableName]['ctrl']['languageField'] ?? [])) {
            return null;
        }

        return (string)$GLOBALS['TCA'][$tableName]['ctrl']['languageField'];
    }

    /**
     * Localize the given record from the parent side of the specific relation.
     *
     * @param array $childElement the database row
     * @param int $targetLanguage the language uid of the target language
     * @param string $parentTable the parent table (the side with the 1 in the 1:n relation)
     * @param string $parentField the column that contains the parent record, which exists on the child table!
     * @param string|null $childrenField the column that contains the child records, which exists on the parent table!
     * If null is given, there is no children field on the parent table, meaning that the relation is only editable in
     * the n:1 direction / from the child side but not from the parent side.
     * @param array $dataHandlerCommands a reference to the {@link DataHandler} command array where the commands,
     * necessary to localize the given record, will be put into.
     * @return bool true if the child record will be localized through its relation with the parent.
     * @throws DBALException
     * @throws Exception
     * @throws \Doctrine\DBAL\Driver\Exception
     */
    protected function localizeRecordFromParentSideOfRelation(
        array $childElement,
        int $targetLanguage,
        string $parentTable,
        string $parentField,
        ?string $childrenField,
        array &$dataHandlerCommands,
        array &$implicitlyTranslatedRecords
    ): bool {
        if (!isset($childElement[$parentField]) || $childElement[$parentField] <= 0) {
            return false;
        }

        $parentRecordId = $childElement[$parentField];

        $parentLocalization = $this->getLocalizationOfRecord($parentTable, $parentRecordId, $targetLanguage);

        // the parent record is already localized
        if ($parentLocalization !== null && array_key_exists('uid', $parentLocalization)) {
            // if there is a children field, we use the inlineLocalizeSynchronize action to localize the child record
            if ($childrenField !== null) {
                if (!is_array($dataHandlerCommands[$parentTable][$parentLocalization['uid']] ?? [])) {
                    $dataHandlerCommands[$parentTable][$parentLocalization['uid']] = [];
                }

                /**
                 * typo3 core data handler commands for the parent localization record
                 */
                $commandForParent = &$dataHandlerCommands[$parentTable][$parentLocalization['uid']];

                // append the record uid to the existing action or create a new one if necessary
                if (!empty($commandForParent['inlineLocalizeSynchronize']['ids'] ?? [])) {
                    // Make sure that the existing command for the parent record is for this children field
                    if ($commandForParent['inlineLocalizeSynchronize']['field'] === $childrenField) {
                        $commandForParent['inlineLocalizeSynchronize']['ids'][] =
                            $childElement['uid'] ?? 0;
                        return true;
                    }

                    // otherwise commit the existing command and override it, since we cannot have multiple
                    // inlineLocalizeSynchronize commands for different fields.
                    $this->commitDataHandlerCommand([
                        $parentTable => [
                            $parentLocalization['uid'] => [
                                'inlineLocalizeSynchronize' => $commandForParent['inlineLocalizeSynchronize'],
                            ],
                        ],
                    ]);
                }

                $commandForParent['inlineLocalizeSynchronize'] = [
                    'field' => $childrenField,
                    'language' => $targetLanguage,
                    'action' => 'localize',
                    'ids' => [$childElement['uid'] ?? 0],
                ];

                return true;
            }

            // with no children field, no special inline localization action is needed.
            return false;
        }

        // the parent record has not yet been localized
        $parentDefaultLanguageRecord = BackendUtility::getRecord(
            $parentTable,
            $parentRecordId
        );
        if ($parentDefaultLanguageRecord !== null) {
            $this->localizeRecordAndRequireParentsRecursively(
                $parentDefaultLanguageRecord,
                $targetLanguage,
                $parentTable,
                $dataHandlerCommands,
                $implicitlyTranslatedRecords
            );
            return true;
        }
        return false;
    }

    /**
     * Fetches a connected mode localization for a given record.
     *
     * The reason this method exists is that {@link BackendUtility::getRecordLocalization()} does not support
     * connect mode localizations on tables that have "l10n_source" configured. Because "l10n_source" does not
     * point to the default language record if the translation was created based on another non-default-language
     * translation, it cannot be used to consistently fetch localizations of a record.
     *
     * @param string $tableName
     * @param int $uid
     * @param int $language
     * @return array|null
     * @throws DBALException
     * @throws \Doctrine\DBAL\Driver\Exception
     */
    protected function getLocalizationOfRecord(string $tableName, int $uid, int $language): ?array
    {
        $languageFieldName = $this->getLanguageField($tableName);
        $translationParentPointerFieldName = $this->getTranslationParentPointerField($tableName);

        if ($languageFieldName === null || $translationParentPointerFieldName === null) {
            return null; // table cannot be localized.
        }

        $backendUserAuthentication = $this->getBackendUserAuthentication();

        $queryBuilder = GeneralUtility::makeInstance(ConnectionPool::class)
            ->getQueryBuilderForTable($tableName);
        $queryBuilder->getRestrictions()
            ->removeAll()
            ->add(GeneralUtility::makeInstance(DeletedRestriction::class))
            ->add(
                GeneralUtility::makeInstance(
                    WorkspaceRestriction::class,
                    $backendUserAuthentication !== null ? $backendUserAuthentication->workspace : 0
                )
            );

        $queryResult = $queryBuilder->select('*')
            ->from($tableName)
            ->where(
                $queryBuilder->expr()->eq(
                    $translationParentPointerFieldName,
                    $queryBuilder->createNamedParameter($uid, PDO::PARAM_INT)
                ),
                $queryBuilder->expr()->eq(
                    $languageFieldName,
                    $queryBuilder->createNamedParameter($language, PDO::PARAM_INT)
                )
            )
            ->setMaxResults(1)
            ->executeQuery();

        $record = $queryResult->fetchAssociative();
        $queryResult->free();

        return is_array($record) ? $record : null;
    }

    protected function getBackendUserAuthentication(): ?BackendUserAuthentication
    {
        $backendUserAuthentication = $GLOBALS['BE_USER'];

        if ($backendUserAuthentication instanceof BackendUserAuthentication) {
            return $backendUserAuthentication;
        }

        return null;
    }

    protected function commitDataHandlerCommand(array $commandArray): void
    {
        $dataHandler = GeneralUtility::makeInstance(DataHandler::class);
        // TODO: shouldn't the data handler be configured in the same way as in
        //       \Localizationteam\L10nmgr\Model\L10nBaseService::_submitContentAsTranslatedLanguageAndGetFlexFormDiff

        $dataHandler->start([], $commandArray);
        $dataHandler->process_cmdmap();

        // TODO: What about data handler errors?
        //       What about Exceptions during data handler processing?
    }
}
