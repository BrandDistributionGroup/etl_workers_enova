# Remove by opis Bilans zamknięcia

COMPANY_DATABASE_LIST = [
    ["SAVVY sp. z o.o.", 'accounts_47_savvy', 'e_5213836340'],
    ["Stavero sp. z o.o. sp.k.", 'accounts_47_stavero_spk', 'e_7010308849'],
    ["Stavero sp. z o.o.", 'accounts_47_stavero', 'e5423295330'],
    ["Brand Distribution Holding sp. z o.o.", 'accounts_47_bdh', 'e_5252249948'],
    ["BDPoland sp. z o.o.", 'accounts_47_bdpoland', 'e_7010494473']
]

ACCOUNTS_47_QUERY = ''' 
--sql
WITH CTE_BI_m_Zapisy_na_kontach AS(
SELECT
    lzk.[ID] AS [IDZapisu], 
    sks.[Symbol] AS [SymbolKonta], 
    sks.[Nazwa] AS [NazwaKonta], 
    sks.[Nadrzedne] AS [IDKontoNadrzedne], 
    sdde.[Nazwa] AS [NazwaDokEwidencji], 
    sks.[Syntetyczne] AS [Syntetyczne], 
    lzk.[Strona] AS [Strona], 
    lzk.[NumerEwidencji] AS [NumerEwidencji], 
    lzk.[NumerDokumentu] AS [NumerDokumentu], 
    szk.[KwotaOperacjiValue] AS [KwotaOperacjiValue], 
    szk.[KwotaOperacjiSymbol] AS [KwotaOperacjiSymbol], 
    szk.[KwotaZapisuValue] AS [KwotaZapisuValue], 
    szk.[KwotaZapisuSymbol] AS [KwotaZapisuSymbol], 
    szk.[RozliczonaKwotaOperacjiValue] AS [RozliczonaKwotaOperacjiValue], 
    szk.[RozliczonaKwotaOperacjiSymbol] AS [RozliczonaKwotaOperacjiSymbol], 
    szk.[RozliczonaKwotaZapisuValue] AS [RozliczonaKwotaZapisuValue], 
    szk.[RozliczonaKwotaZapisuSymbol] AS [RozliczonaKwotaZapisuSymbol], 
    szd.[DataPodatkowa] AS [DataPodatkowa], 
    IIF(lzk.[Strona] = 1, szk.[KwotaOperacjiValue], 0) AS [Wn_KwotaOperacji], 
    IIF(lzk.[Strona] = 2, szk.[KwotaOperacjiValue], 0) AS [Ma_KwotaOperacji], 
    sks.[Typ] AS [Typ], 
    sks.[TypTekst] AS [TypTekst], 
    sks.[Rodzaj] AS [Rodzaj], 
    sks.[RodzajTekst] AS [RodzajTekst], 
    sks.[ElementKsiegowalny] AS [ElementKsiegowalny], 
    sks.[ElementKsiegowalnyTekst] AS [ElementKsiegowalnyTekst], 
    lde.[Podmiot] AS [IDPodmiotu], 
    lde.[PodmiotType] AS [TypPodmiotu], 
    lzk.[ZapisZamkniecia] AS [ZapisZamkniecia], 
    sof.[ID] AS [IDOddzialu], 
    sof.[NazwaSkrocona] AS [NazwaSkroconaOddzialu], 
    sof.[Nazwa] AS [NazwaOddziału]
FROM 
    [link_ZapisyKsięgowe] lzk
INNER JOIN [hub_Konta] hk ON lzk.[Konto] = hk.[ID]
INNER JOIN [sathub_Konta_szczegóły] sks ON hk.[ID] = sks.[ID]
INNER JOIN [hub_DefDokEwidencji] hde ON lzk.[DefinicjaEwidencji] = hde.[ID]
INNER JOIN [sathub_DefDokEwidencji_szczegóły] sdde ON hde.[ID] = sdde.[ID]
INNER JOIN [satlink_ZapisyKsięgowe_daty] szd ON lzk.[ID] = szd.[ID]
INNER JOIN [satlink_ZapisyKsięgowe_kwoty] szk ON lzk.[ID] = szk.[ID]
LEFT OUTER JOIN [link_Dekrety] ld ON lzk.[Dekret] = ld.[ID]
LEFT OUTER JOIN [satlink_Dekrety_daty] sdd ON ld.[ID] = sdd.[ID]
LEFT OUTER JOIN [satlink_Dekrety_Wn/Ma] sdwm ON ld.[ID] = sdwm.[ID]
INNER JOIN [hub_OkresObrachunkowy] hoo ON lzk.[Okres] = hoo.[ID]
INNER JOIN [sathub_OkresObrachunkowy_szczegóły] soos ON hoo.[ID] = soos.[ID]
LEFT OUTER JOIN [link_DokEwidencja] lde ON ld.[Ewidencja] = lde.[ID]
INNER JOIN [satlink_DokEwidencji_daty] sddo ON lde.[ID] = sddo.[ID]
INNER JOIN [satlink_DokEwidencji_szczegóły] sdso ON lde.[ID] = sdso.[ID]
LEFT OUTER JOIN [hub_Banki] hb ON (lde.[Podmiot] = hb.[ID] AND lde.[PodmiotType] = 'Banki')
LEFT OUTER JOIN [hub_Kontrahent] hknt ON (lde.[Podmiot] = hknt.[ID] AND lde.[PodmiotType] = 'Kontrahenci')
LEFT OUTER JOIN [hub_Pracownik] hp ON (lde.[Podmiot] = hp.[ID] AND lde.[PodmiotType] = 'Pracownicy')
LEFT OUTER JOIN [hub_UrzędyCelne] huc ON (lde.[Podmiot] = huc.[ID] AND lde.[PodmiotType] = 'UrzedyCelne')
LEFT OUTER JOIN [hub_UrzędySkarbowe] hus ON (lde.[Podmiot] = hus.[ID] AND lde.[PodmiotType] = 'UrzedySkarbowe')
LEFT OUTER JOIN [hub_ZUS] hzus ON (lde.[Podmiot] = hzus.[ID] AND lde.[PodmiotType] = 'ZUSY')
LEFT OUTER JOIN [hub_OddziałyFirmy] hof ON hk.[IDOddzialu] = hof.[ID]
LEFT OUTER JOIN [sathub_OddziałyFirmy_szczegóły] sof ON hof.[ID] = sof.[ID]
),
CTE_features_zk AS (
SELECT f.Parent, TRY_CAST(REPLACE(f.[DataKey], ' ', '') AS INT) AS DataKey
FROM [Features] f
WHERE f.ParentType = 'ZapisyKsiegowe'
),
CTE_mpk_and_id AS (
SELECT 
fzk.Parent,
fzk.DataKey,
ck.ID AS mpk_id,
ck.Nazwa AS mpk
FROM CTE_features_zk fzk
LEFT JOIN [CentraKosztow] ck
	ON ck.ID = fzk.DataKey
),
CTE_okres AS(
SELECT
	zk.[Data],
	z.SymbolKonta,
	z.NazwaKonta, 
	k.Nazwa,
	zk.NumerDokumentu, 
	z.NazwaDokEwidencji, 
	z.Wn_KwotaOperacji,
	z.Ma_KwotaOperacji,
	z.RozliczonaKwotaOperacjiSymbol,
	mpk_id.mpk
FROM [CTE_BI_m_Zapisy_na_kontach] z
LEFT JOIN [Kontrahenci] k
	ON k.ID = z.IDPodmiotu
LEFT JOIN [ZapisyKsiegowe] zk
	ON zk.ID = z.IDZapisu
LEFT JOIN CTE_mpk_and_id mpk_id
	ON mpk_id.Parent = z.IDZapisu
WHERE (z.SymbolKonta LIKE '7%' OR z.SymbolKonta LIKE '4%')
)
SELECT 
	o.[Data],
	o.SymbolKonta,
	o.NazwaKonta, 
	o.Nazwa,
	o.NumerDokumentu, 
	o.NazwaDokEwidencji, 
	o.Wn_KwotaOperacji,
	o.Ma_KwotaOperacji,
	o.RozliczonaKwotaOperacjiSymbol,
	o.mpk,
    '{company_name}' AS firma
FROM CTE_okres o
;
'''

