import wg_api
import config
import sqlite3
import datetime
import logging
import math

db_name = "event_dec_2015"
conn_db = sqlite3.connect(db_name + ".sqlite3")
curs_db = conn_db.cursor()
clans = ["XG","XG-A","XG-T","EQ","CPA","OS_H","PAKU","TOP-A","ACE-S","EXE","3AKOH", "PC","SLM","-NO-","PX_TM","HARDA","BOSS",
         "GWARD","AIR","DALE"]
clans_with_more_than_50_members = [u'3AKOH', u'OLD', u'STL', u'XG', u'OWL_', u'DALE', u'MERCY', u'CCCP', u'WB', u'THE_M', u'SLM', u'GW', u'EQ', u'PSPL', u'4PDA', u'RED', u'72RU', u'USSR', u'51R', u'RUS', u'59RUS', u'SPR', u'YKT', u'FURRY', u'PKKA', u'CP', u'CPA', u'BM', u'PC', u'BULA', u'BLITZ', u'ACE-S', u'TVER', u'VP', u'KILER', u'RU', u'WOTBC', u'EXE', u'DS', u'RTD', u'KURSK', u'BRO', u'WAR', u'OMSK', u'LION', u'T-MAN', u'LE_FI', u'RUSS', u'DV', u'OTB', u'RR', u'ALT', u'NORDW', u'DRG', u'TANK', u'ONE4', u'DEAD', u'RUS1', u'SGR', u'WWE', u'45RUS', u'NEO', u'BELG', u'MSK', u'BEARS', u'TOMSK', u'WOTVK', u'TEPEK', u'_ZOO_', u'WOTB', u'VN', u'IMP-1', u'EHM', u'02KAZ', u'-V34-', u'UKR1', u'CSPN', u'_VIP-', u'KZ', u'MARS', u'RED_R', u'WGTB', u'BLG', u'EKB96', u'68RUS', u'FOX1', u'_TDT_', u'YKT-B', u'-AYE-', u'TLT', u'RUS74', u'NAG', u'102', u'74RUS', u'ELITE', u'RUSSI', u'18RUS', u'RAKU', u'63RUS', u'KBR', u'MSK77', u'EKB', u'PENZA', u'-S-', u'WIK', u'PIMP', u'1970', u'-PWNY', u'MINSK', u'REG49', u'TANKS', u'54RUS', u'LISIY', u'VS', u'TM', u'_XXX_', u'KIL', u'N2G7K', u'PK08', u'BZS', u'89RUS', u'15RUS', u'9_MAY', u'FORCE', u'DAG05', u'TATAR', u'_SPA_', u'ST0P', u'TBK', u'SKY', u'77777', u'CLAN', u'FSSTR', u'SPB78', u'OSK', u'REG73', u'_S_T_', u'JIEB', u'-50', u'KR', u'VOIN-', u'DD', u'555', u'KIEV', u'-MSK-', u'OREN', u'CRRUS', u'ATOH', u'VLG', u'27', u'KD', u'_WOC_', u'RUS23', u'GLBR', u'ONE10', u'_SSSR', u'EXP', u'VE_LU', u'NET', u'GORA', u'NA_VI', u'SAS', u'33RUS', u'1806', u'RST', u'B_O_S', u'61RUS', u'VIP', u'GLAFI', u'XMAO', u'73RUS', u'SNOW', u'28RUS', u'PNZ58', u'RAKAL', u'VHL', u'QVIR', u'RU1', u'FSB', u'BIT63', u'KISS', u'IS4', u'CANKE', u'LV', u'_WOT_', u'_GO_', u'CCCP-', u'1943', u'RP', u'PRIDE', u'PNZWF', u'RW', u'YAR', u'RZN_0', u'KTM', u'VSD', u'KAZAN', u'VDK', u'PERM', u'YOPTA', u'KVKZ', u'MSC', u'YKT14', u'30RUS', u'74_RU', u'BIA', u'BLR', u'LUBER', u'WOTUA', u'COIO3', u'PZDV', u'BRY', u'KMS', u'SP163', u'ZSU', u'B-L', u'XG-A', u'RUS_1', u'PAK', u'H_WOT', u'TROYA', u'WRGM', u'RB', u'D-ION', u'NG-1', u'WOT_', u'KAZAH', u'FTS', u'WOTMD', u'LIDER', u'SEW', u'EVERS', u'HERO', u'164', u'KRSK', u'RU52S', u'NN0V', u'BUR', u'CT-4', u'WH1NE', u'LS', u'HAGE', u'DNO', u'CHE35', u'CBAT', u'OS_M', u'RUS98', u'JOWE', u'-05-', u'_TT_', u'DAWI', u'22TA2', u'UKR0P', u'2001', u'NOBAD', u'GTA', u'RU11', u'SAKHA', u'WOS', u'PTT', u'RUS08', u'03RUS', u'AZAT', u'BLEKS', u'RUS88', u'XXXX', u'T_G_C', u'-SPB-', u'KG_39', u'MAGMA', u'ARD', u'52RUS', u'JOVE', u'ODSK', u'RND61', u'DVF', u'WOTR_', u'WAR_', u'GML', u'-DVR-', u'_BRO', u'CTYK', u'BLIZ', u'BDB', u'RFF', u'GROZA', u'LGD', u'KS', u'66RUS', u'EV1L', u'TNKS', u'HC', u'FURSO', u'SHOT1', u'1925', u'502', u'RUS01', u'BIO', u'RAKY', u'_IZH_', u'MIGHT', u'AZE', u'262', u'1944', u'052RU', u'RCRUS', u'MEM', u'BLAGO', u'NAS', u'RASIA', u'1-1', u'56', u'PING', u'AVG', u'ATAKA', u'VDVSH', u'NSK54', u'HAST', u'BRG', u'ARGUM', u'3OHA', u'BEL1', u'GLEAM', u'OMEGA', u'VORON', u'WAN', u'DESQ', u'WF', u'NOOB', u'B0SS', u'CTABP', u'GOMEL', u'RU32D', u'ZLO', u'KSM', u'ODVL', u'TCP', u'UT', u'P-R-O', u'NAVIC', u'CCCP1', u'CHR95', u'KSTS', u'KZ777', u'DNG', u'-BT-', u'INFIN', u'U_K_R', u'BLOLF', u'030RU', u'L1VE', u'TOP-A', u'_KGZ_', u'DEN', u'LIGA4', u'PITER', u'OTRUS', u'1VIP1', u'ACES2', u'_KH_', u'RPG77', u'TAWER', u'_FBI_', u'TPL', u'BELL', u'3485', u'EXILE', u'TRC_', u'WOT1', u'OTHER', u'ONEN2', u'1RND1', u'IMBA', u'RUS_8', u'4SIDE', u'LOM', u'GLAF2', u'HARD', u'NERAK', u'AQWA', u'R6', u'BREST', u'DNBAS', u'EKBII', u'KRESH', u'GDU', u'NSK1', u'_PPS_', u'U777A', u'DARIY', u'PR0', u'PAKU2', u'JAN84', u'-TGC-', u'CATS', u'TTANS', u'161', u'RATI', u'RAKIB', u'WF201', u'LWT', u'BK', u'ALM', u'DHTM', u'HP', u'VRN', u'WOT_X', u'60OPS', u'RA181', u'KRRUS', u'RONIN', u'AD', u'GODS', u'STAL', u'NVRSK', u'KZZ', u'36', u'A-ONE', u'BOS', u'USSR9', u'TBS', u'FZ', u'JUPIT', u'TK-9', u'4BRIG', u'MECH', u'38RUS', u'DOK', u'MAN', u'STBRO', u'KOD', u'6996', u'GO2D', u'BBOW', u'OMSK_', u'HAI', u'P0WER', u'STARS', u'S_W', u'MOLOT', u'KUBAN', u'ALPHA', u'LVIV', u'TFK', u'LKG', u'RUS02', u'GOW', u'KFC', u'ROTA', u'YKROP', u'ERIO', u'OS', u'STD', u'10FG', u'LO-SI', u'PAKU', u'RU007', u'JTE', u'K1NGS', u'78', u'HBPCK', u'MONST', u'RUS38', u'BOSSS', u'OLDSC', u'PTSAY', u'72RUS', u'NS', u'116', u'CHEL', u'HAL', u'SNV', u'CRL', u'KUKIS', u'RUS13', u'FIGA', u'TRR', u'CHUV', u'_Z-S_', u'36RUS', u'AZE-1', u'IBRUS', u'64RUS', u'ARM_1', u'DONBA', u'SEWS', u'STP', u'A-FX', u'DAGI', u'DW', u'SA_KH', u'RWS', u'1981', u'AAAA', u'BATL', u'KRUTO', u'RUS27', u'37RUS', u'MYUA', u'CIR6_', u'R_A_K', u'M_T_I', u'GO777', u'MANS', u'-WAR-', u'LOBVA', u'LPLK', u'WOTNG', u'PRO20', u'CSKA', u'N_S_K', u'41RUS', u'RUS_X', u'BWAR', u'COW', u'BELI', u'W0LF', u'DOBRO', u'FU_RY', u'-UKR-', u'MAYKO', u'KOL', u'ONE17', u'LV_UA', u'TANK5', u'SAR64', u'ONLIN', u'URALW', u'HRDEW', u'S_O_S', u'KLAN8', u'ETETR', u'88', u'RUS24', u'SASHA', u'RT116', u'61', u'FRND', u'MAN-X', u'R_A', u'KD05', u'LIT', u'ZVERI', u'FLEN', u'86RUS', u'SWAP', u'E-100', u'9DEN', u'STEEP', u'BLR1', u'AS15', u'RUS46', u'KZ013', u'AMWAY', u'GLOR', u'-RB-', u'WTFW', u'UKR33', u'CRETS', u'RANGO', u'_CCCP', u'FOR_E', u'RMC', u'FRONT', u'WARS', u'RES', u'UFA02', u'-STR-', u'33RU', u'SVAT', u'W1W2', u'34RU', u'ADK', u'BAKU', u'DFUA', u'MOTYA', u'MTA', u'MRAND', u'S-P-B', u'34VLG', u'038RU', u'82RUS', u'OS_G', u'SMTR', u'FSB-7', u'K_INT', u'KIROV', u'INTER', u'VOLOT', u'REDS', u'BLIZ_', u'OSH05', u'PVP', u'UA_EU', u'VMF', u'-KGB-', u'GDT', u'BGM', u'TMAF', u'KBK', u'RUS72', u'BNDT', u'T_B_W', u'ROMIK', u'174RU', u'MP3', u'GEO', u'24RUS', u'O-UA', u'_FURY', u'OMON', u'Q_WAR', u'PAMIR', u'DHP', u'GER', u'BANDA', u'KPRF', u'AI2', u'5FIVE', u'CKP', u'BORZ', u'B_L', u'FUN', u'NUBY', u'KG001', u'UNITY', u'NOB', u'SKP', u'RUS00', u'WGS', u'KSMAN', u'FCSM', u'ZEV', u'YEPT', u'PUMA', u'CENTR', u'RUS_R', u'BY-02', u'R-A', u'WOR', u'SPBRU', u'KB-T', u'BT-WI', u'IRONF', u'BREAK', u'SVBD', u'NNOV', u'ARM1', u'TT_UA', u'RUS25', u'JAH', u'21354', u'TULA', u'RU_PT', u'GRY', u'REAL', u'TOR56', u'WOT12', u'GOLDA', u'63RU', u'GOLD3', u'_UFA_', u'BISON', u'IREN', u'PRM', u'IGERI', u'RDB', u'A-K-N', u'42RUS', u'GLORY', u'MGLS', u'VOIIN', u'OSMIN', u'ZELAO', u'NOVO', u'TTT', u'T-74', u'18REG', u'HNHR', u'7-7-7', u'NOVRU', u'NURIK', u'_BOP_', u'FEAR', u'OPSS-', u'95', u'8GR', u'63REG', u'DEU', u'NUMO1', u'174', u'DUB', u'KAZ-1', u'49RUS', u'TVER1', u'T-NSK', u'AZNAK', u'UFA10', u'FERS', u'CJIK', u'RAKI_', u'KRIG', u'DAST', u'VIP10', u'86', u'AN_TI', u'MAPKA', u'CUB', u'_USSR', u'L1', u'VPRO2', u'CAXA', u'BOGT', u'RUS8', u'WELCO', u'UKR2', u'DREV1', u'UA-UA', u'18-RU', u'-WF-', u'2808', u'VVVVV', u'WOIN', u'F0SHE', u'APM', u'ADMIN', u'MDR', u'-GRAD', u'JAN', u'SUN', u'GAPON', u'-BY-', u'GBR', u'FRAER', u'C_O_C', u'DO3OP', u'KZN16', u'_102_', u'P_T', u'RIF', u'VIP31', u'SPORT', u'161R', u'XXXL_', u'INTRO', u'FIRE7', u'WIJDH', u'MR153', u'KWUA', u'HAIL', u'MAX11', u'USSR_', u'HWAR', u'SKB', u'TJ', u'A30B', u'BTV00', u'OR96', u'UZWOT', u'BST', u'9999', u'SSBRO', u'KRF', u'VAD77', u'OPEX', u'MDMA', u'NITMR', u'TUMEN', u'SPMSK', u'58PNZ', u'__UFA', u'SCHTR', u'WOLF9', u'SVORA', u'KZ-13', u'T-MAH', u'RRR', u'A_RU', u'TRI3', u'BIG4I', u'NOCHI', u'_26_', u'_NR_', u'STISG', u'TTPRO', u'RUS35', u'IDF', u'3737', u'KSP', u'REALL', u'RUS_V', u'VERON', u'GTUA', u'RG', u'SVL', u'NARA', u'29RUS', u'P_H_M', u'29REG', u'SMTRU', u'EDD', u'DNBS', u'101', u'SPBR', u'KGRB', u'RUS40', u'37', u'6161', u'VLUPI', u'TANKI', u'ODUA', u'VW', u'-SD-', u'CTAD1', u'-666-', u'N_V_2', u'UF', u'X777X', u'FURY-', u'LEGEN', u'DRAGO', u'N1GO', u'REP', u'LOKO1', u'R62', u'RUS51', u'ARMY-', u'RASHA', u'CHIST', u'DPUA', u'N4', u'K_R', u'SILA_', u'JDM', u'NITRO', u'TEAMS', u'OSETI', u'KMC', u'KZ15', u'FNAT', u'ZT', u'ALTAY', u'05RU', u'VSUKR', u'OO5', u'ORSK', u'VYA', u'GRUPZ', u'CSKA_', u'KZ_04', u'BUI', u'WOTBY', u'WTBZ1', u'GUD', u'SPARK', u'PX_TM', u'TIMIT', u'KHV27', u'PRO10', u'JLT', u'CAXA_', u'VCLAN', u'BY-3', u'ALMET', u'BLRM', u'ANMLS', u'ZEN12', u'VERS', u'39RUS', u'TTC', u'WAR10', u'NOCHE', u'KHRKV', u'RUSII', u'VORLD', u'KLG', u'PSP', u'YGRA', u'52RU', u'NVK', u'61RU', u'MAI45', u'LVF', u'PYCCK', u'PERM2', u'REG23', u'116RU', u'RUS_5', u'RICHI', u'NUKS', u'VIP77', u'HEROY', u'TANCH', u'STRAX', u'MCS', u'OBL', u'W_O_N', u'SCLAN', u'46RUS', u'SAU', u'_RU_', u'ZXCV', u'RU_BL', u'PEA', u'SLG', u'P_Z_P', u'KVO', u'GANGA', u'KLG40', u'WOR_1', u'IF', u'VLAD_', u'X-ART', u'DRZ1', u'LOFTA', u'AM', u'RU_56', u'S-OR', u'KHVDV', u'GOBRO', u'WOT11', u'HETZE', u'PAV', u'VOIYN', u'RU-05', u'37A', u'W_T_W', u'76YAR', u'RVB', u'B_SH', u'BIS', u'UA_X', u'RU_62', u'CH100', u'RUTAN', u'WPRID', u'_NIKE', u'SERG-', u'BOICY', u'IRONN', u'13RU', u'ASTRA', u'C_Y_C', u'RBEAR', u'M51', u'ONE38', u'57RUS', u'YTA', u'OT163', u'VFF', u'1S_1K', u'TAGIL', u'BANZA', u'NOGOD', u'TOP-M', u'MOW', u'VINER', u'LEV', u'VSSSR', u'WOTNJ', u'_VOIN', u'DOBRI', u'SDF', u'CMERT', u'UA_GE', u'GRIN', u'-ROSS', u'31RUS', u'BY-', u'SLW', u'DIN', u'10RUS', u'TOP_1', u'_GTU_', u'AR-TA', u'TOHHA', u'TRKUA', u'PDM', u'TTS1', u'VTK', u'TNVD', u'ROX', u'-EVIL', u'KZN1', u'AFGAN', u'CS', u'Y-RAK', u'9POTA', u'N178', u'_STD_', u'ONE15', u'NORD-', u'C18', u'SIL', u'TRDSA', u'ALPFA', u'SML67', u'RUS-', u'SV16', u'WTGO', u'EMIR', u'152', u'3CP', u'SW64', u'G_T_F', u'08', u'-R_M-', u'LTC', u'SJQ82', u'RUSPB', u'B_S_M', u'_C_', u'VIP5', u'RUSKE', u'ONE7_', u'KZ02', u'NRL', u'AMUR-', u'SIX', u'BOLT', u'HFM', u'RSY', u'REG2', u'REG76', u'XXXXX', u'KARAT', u'96RUS', u'89RU', u'M_L_P', u'NK', u'KOPM_', u'CYTS', u'GSM', u'PBCH', u'-O_O-', u'MRT', u'43', u'_DPR_', u'DAYZ', u'-D-T-', u'1974', u'BATIR', u'MAK', u'TOLPA', u'CSGO', u'KAT', u'1971', u'KV2', u'UZCOM', u'RUSS2', u'KV', u'NKN', u'DEDBY', u'DALE2', u'SKIF', u'163RU', u'_FAN_', u'ASDFG', u'UAWIN', u'LMA', u'64SAR', u'USSRR', u'MRSHL', u'SOUP', u'STG_', u'MV', u'_UKR_', u'R-SR_', u'EKX', u'WARZO', u'STQRM', u'YYC', u'19455', u'-ZKD_', u'V_I_O', u'FREE_', u'SERGE', u'DNEHE', u'CREW', u'TLWOT', u'SERI', u'312', u'IZMOR', u'STAIA', u'SVRG', u'72-RU', u'NUBAS', u'NOV', u'ARMA', u'QWER2', u'PIO6_', u'URAL9', u'RSIP', u'80LVL', u'KZ_AK', u'FBT', u'SLV', u'125RU', u'DOM3', u'MOSC', u'ARM_', u'-RDC-', u'REG08', u'WOT01', u'VLZ', u'ATAS', u'41REG', u'WOIFS', u'CGB', u'SPB1', u'MSV', u'186', u'B-T-W', u'GIGN', u'53729', u'OTB_1', u'RF161', u'ULTRA', u'S_PRO', u'PRO23', u'66RU', u'OMK55', u'OM0H', u'_PA_M', u'BARCA', u'WARRS', u'WAR20', u'WOW3', u'FRAGI', u'NEMAN', u'STGD', u'BRONY', u'-NO-', u'HOST', u'FURI', u'RAIN', u'AC-UA', u'KGN45', u'12346', u'RSN', u'T16', u'HGER', u'WBLC', u'-WT-', u'E-D', u'LIMP', u'ICCCP', u'2322', u'T54', u'2005', u'_TLT_', u'-U-', u'BU', u'GOLDD', u'S67', u'_KRG_', u'VETER', u'DEV66', u'RBG', u'_RPG_', u'WOT4', u'STOIC', u'DEN37', u'R-A-V', u'ROMB', u'WERY', u'DOS01', u'KMS27', u'ORK', u'TRFCT', u'FOH', u'KTTC_', u'TWG', u'65432', u'DIE', u'UKR-1', u'MBRO', u'KXL', u'VIP0', u'A-ATA', u'SB777', u'FLNT1', u'YARSL', u'CAT_', u'WBR71', u'COVEN', u'ARH-G', u'SE', u'DPR1', u'MIG-T', u'UZB_1', u'ARMIS', u'XXXFB', u'ASTAN', u'-KOT-', u'GANER', u'FLINT', u'AD-55', u'VDV78', u'ATILA', u'_PERM', u'7P', u'SD999', u'VATA', u'GOD1', u'REG68', u'PJK', u'T_P_M', u'58', u'RD-RU', u'UFARB', u'GGIZI', u'HASKA', u'WILD4', u'DIP', u'SN579', u'MRBM', u'SJFJJ', u'BY-L', u'EONE1', u'SDV36', u'U-SIB', u'GR_C', u'-1943', u'_S_B_', u'ANI_1', u'ZPUA', u'-USSR', u'WOT-C', u'JUMBA', u'JSI', u'ST_BR', u'KS63', u'KAPA', u'ODI', u'SNAGA', u'CKBO', u'S1945', u'UA19', u'VIVAT', u'AVT52', u'RUS97', u'MEGO', u'NINJA', u'EVG--', u'MKT', u'POFIG', u'GWARD', u'SH1', u'P_B_', u'PROF1', u'BATTL', u'UA918', u'TB6', u'KVN', u'WOT--', u'BO-ST', u'AZE-2', u'7788_', u'WEST0', u'MDB', u'156RU', u'OEGK', u'SOBR3', u'ZIPA', u'SINS', u'AVTB', u'MYASO', u'URSA', u'123EJ', u'NEBO_', u'MIKE', u'-KZ', u'_OFF_', u'16KZN', u'VOLF', u'192-6', u'WTB3', u'GTGTG', u'APTA', u'LOTUS', u'ION', u'TERRO', u'-BLR-', u'SOZH', u'-WIN-', u'SKY_2', u'CUMA', u'PRO72', u'RRR2', u'APA4E', u'ONE-H', u'T-34-', u'SKBS', u'VUDGI', u'CRAZ', u'WOTS', u'RUS54', u'JAF', u'ZALPP', u'MERCE', u'LP', u'P_COC', u'VL_25', u'RUS03', u'40RUS', u'OREN7', u'A_WOT', u'BEL32', u'FREEW', u'SSP', u'BLRUS', u'K0PM', u'VOR_1', u'-M_T-', u'ATL', u'KILLA', u'_CFG_', u'ALNOS', u'PUT1N', u'RUSK', u'MOZYR', u'350Z', u'NAVI3', u'HQ', u'REG95', u'14YKT', u'MIF', u'WERUS', u'_UKR', u'42YTR', u'GTR7', u'_FCSM', u'YOLA', u'BH316', u'VIP_', u'UA_2', u'RIM', u'WBW', u'-AM-', u'BRO1', u'4ONE4', u'NAEZD', u'GANSM', u'LAZ', u'PLAY_', u'7BAD', u'CKILL', u'KIEVN', u'SITI', u'T228A', u'UA_99', u'-P-', u'HVALA', u'116R', u'_BTP_', u'NO-1', u'BKILL', u'CRS-1', u'MOYU', u'RKUST', u'MULTI', u'LOL_R', u'JYH', u'RUSES', u'_ACES', u'PILKA', u'2014', u'SRVA', u'SPOK', u'4-10', u'UKR-7', u'DDOS', u'SHE', u'WGT', u'KUSH', u'21AGE', u'GRZ', u'ERK', u'KIS', u'MOONS', u'-UKR', u'HP-64', u'RU124', u'8912', u'BTV', u'BKTB', u'RVG', u'NASHI', u'RU65', u'NRAID', u'UKRVS', u'PACA', u'ZLO88', u'BIO_Z', u'DENER', u'GWIN', u'V7M6', u'UTANK', u'NNAVI', u'TANK3', u'0X0TA', u'PSKOV', u'4-FUN', u'96', u'MIKO', u'BSTAR', u'RW32', u'ANLV', u'KOT1', u'NCM', u'AUDI', u'24RU', u'AROWS', u'XG-R', u'STEEI', u'RUSSD', u'_NDS_', u'SART', u'ROSIA', u'ALASH', u'XTM', u'YKP', u'BOIKA', u'CCCP5', u'YTHVN', u'PAK_', u'AZM', u'L-R', u'LIGHT', u'SSS22', u'_SNP_', u'22_RU', u'WE-UA', u'--__', u'RMSD', u'SILA1', u'MBV3_', u'MAUS_', u'_SKA_', u'_RUSB', u'0777', u'RAKOK', u'ATOM_', u'SK15', u'SUV', u'TMA', u'1141', u'18IZH', u'90RUS', u'27DV', u'SOFT', u'HEROU', u'2_GO', u'NINE9', u'13RGN', u'ACT', u'ARTYR', u'ILYA4', u'NKVG', u'OMEG', u'ANTIB', u'JOI', u'TOPAZ', u'GHY', u'BLIPZ', u'ELITA', u'WOTAR', u'B-I-G', u'79R', u'OREN-', u'BYBLR', u'CH-OB', u'EYE', u'HOT_P', u'RUS73', u'IRK38', u'_ZZZ_', u'RA41', u'-DON-', u'BAD_D', u'NSO54', u'SBFG', u'MSH', u'NGN', u'GAF', u'-ALD-', u'W_MAN', u'FUGAZ', u'URAL_', u'BMWX6', u'2936', u'T-62A', u'DORKI', u'RL21', u'NR', u'SPB77', u'GURUS', u'--UA-', u'23145', u'SAA', u'RUP', u'FHR', u'RAI', u'NGBT', u'TPK', u'P62', u'OE09', u'UKRAS', u'GTA_5', u'SP-TA', u'RSSSR', u'WOT', u'-RA-', u'IS-7', u'HVOST', u'ZEN37', u'BEREG', u'HIOPA', u'ZUBR_', u'36-RU', u'CHE74', u'OME2', u'TBW', u'DON61', u'TREAL', u'BPZ', u'_M_G_', u'GRN', u'AGR_A', u'OTAKY', u'VBD', u'SEDAN', u'159', u'XZKTO', u'TURKS', u'MB-15', u'PAIN1', u'SZONE', u'POOL', u'IB3', u'ONLY_', u'TD-71', u'FCKK', u'ASD99', u'TYAN', u'KLD', u'STVOL', u'KG_RU', u'RU14', u'G_P_N', u'TVR69', u'BODIY', u'SSSR', u'ATTA', u'69RUS', u'KIIL', u'KBECT', u'REEED', u'LTE4G', u'TEKA', u'71REG', u'GYM', u'TEN', u'F-S-B', u'FG78', u'HOROR', u'SIB22', u'ORDER', u'202A', u'ALBIN', u'IRKUT', u'KLAN', u'UFA1', u'AD_15', u'AMBER', u'BPO16', u'RPPG', u'WON', u'_HELL', u'HOB', u'B26', u'BOG21', u'DOFEX', u'GTP', u'ROGAN', u'NGHB2', u'AMG5', u'ATKU', u'WATT', u'RUS68', u'KU_GA', u'VELBO', u'GE95', u'TAHK1', u'SP_KL', u'SDEK', u'VO', u'WA', u'STZ', u'ZIPS', u'KZ-1', u'RUSGQ', u'TOR2', u'WWWT', u'VELEZ', u'101L', u'QVEK', u'TGKA', u'XEVI', u'WARSA', u'EWDY', u'BEL_1', u'ARMY1', u'SGEDR', u'KOT_3', u'RAKI-', u'YKR', u'YOUI', u'EA37', u'GHFJV', u'GIVES', u'TOPST', u'S-G', u'GFD95', u'BAS1', u'ASERT', u'TAB95', u'_S_12', u'V33', u'SL27', u'RUPWF', u'AFN', u'TADT_', u'COWOT', u'WINGS', u'256FR', u'_THE_', u'FKHD', u'ZXCVB', u'PSS', u'ENER', u'ONER5', u'DBMSK', u'1945-', u'ONE1E', u'KNU', u'CHEST', u'CADOF', u'O_SP', u'SM_UA', u'1989I', u'WRG-N', u'UA__', u'-FSB-', u'SSR12', u'ZXCV_', u'EKB3', u'HEO', u'R-B', u'ZVER-', u'RIDER', u'RKO', u'T34-', u'BLEC', u'VASII', u'L0L', u'SURPA', u'-KOH-', u'RUS_-', u'CAXA-', u'TREEK', u'GT_GA', u'URFO_', u'-STSH', u'APALA', u'DON_T', u'KG220', u'SVA', u'KZ09R', u'GVSR', u'22752', u'_G0_', u'AYA', u'FIR', u'PERES', u'ZEIEK', u'VEKKO', u'FNAF', u'-220B', u'BLRRU', u'55551', u'RTT', u'SGRDS', u'AYE64', u'MIN_K', u'FKSV', u'KHAT', u'MOB', u'22OUT', u'COM-2', u'BODY', u'-GOD-', u'FPCPP', u'DNIWE', u'COB', u'WOT63', u'DAYN', u'_NGB_', u'43RU-', u'55533', u'PD23', u'WHP', u'SF', u'VIPRU', u'-FN-', u'HYTYG', u'EVENS', u'SAT13', u'AYE13', u'WWILD', u'XAYS', u'MAGA', u'YS72', u'0365', u'GEROY', u'_PAK-', u'J632', u'LIT15', u'ESAUL', u'BOY1', u'D_T_P', u'ST34', u'_CK_', u'R14', u'154', u'JAGRY', u'UA_CH', u'VIK2', u'MARA', u'CHE25', u'GIRAF', u'33G', u'32RU', u'TBKT', u'GUN99', u'MX', u'RAE', u'LADYS', u'LBX', u'EGAPA', u'-T34-', u'ZARTA', u'BAM_', u'LW', u'VSS11', u'TL-A', u'_MFW_', u'SWB', u'1REG1', u'SOV', u'ONE-3', u'LEA', u'ZK-1', u'6GB66', u'_SRT_', u'STAR5', u'MEB', u'BRBS', u'8917', u'GS1', u'KZ7', u'SED7', u'HOSTO', u'50LOL', u'BROWN', u'BL00D', u'UZZZ', u'OOSPN', u'BT1', u'VMS', u'-VIP', u'ZAO1', u'AGRR', u'MUSS', u'XP282', u'RAPER', u'MAHOO', u'UA_UA', u'SKO', u'B-O-G', u'YEAH', u'4G_KN', u'__VVG', u'WGWB', u'78178', u'_-I-_', u'BLOHI', u'IDMRL', u'GNEW', u'ST-PB', u'S-TA', u'AK', u'GFR', u'STAF', u'SPOT', u'MBTO', u'UHTA', u'7-RUS', u'VIPKL', u'XXX7', u'_GG_', u'VESA', u'XFBIX', u'WGTV', u'MAX1', u'STV', u'21V', u'ARES', u'OK246', u'GARIK', u'SLAV', u'L-VND', u'RAC', u'LUMIN', u'228_', u'DLF', u'116CH', u'JAR', u'GWPT', u'96EKB', u'VSHOT', u'LOR', u'_LOL_', u'CLEAR', u'SZKO', u'ZLL', u'TGZ', u'AGORA', u'DON1', u'PTASH', u'GT-90', u'DREX_', u'MINOB', u'GOPI-', u'KAMI1', u'NF_32', u'DIFOR', u'AAM', u'28282', u'0IDHD', u'DRUID', u'100TH', u'TEA', u'BST_D', u'GEN3', u'G2E-R', u'2_BY_', u'BCCCP', u'EX-Q', u'A_KMS', u'BALDA', u'13VIS', u'RALEX', u'_KING', u'12_3', u'362', u'21TB', u'A4K0_', u'TIA', u'ARM39', u'DEIT', u'_FCB_', u'-EK', u'CEZI', u'TBH', u'FRE32', u'_ROTA', u'QLW', u'A-Z-A', u'GRENA', u'_116_', u'AFD', u'SPAS', u'PECHI', u'W_A_P', u'DESTR', u'BLR-2', u'RB123', u'ZETIS', u'T-I-R', u'-DMG-', u'SHIBA', u'25RUS', u'86905', u'MANAS', u'DN_P', u'NWR', u'ZFV', u'TEXT', u'DISCO', u'ELS', u'SEV-6', u'VDRB', u'OZI_1', u'UBRA', u'_TOG_', u'WEWEN', u'B-F', u'TANC1', u'2701', u'Z____', u'WDL', u'84622', u'BLAR', u'MANS_', u'OME_0', u'ARMIA', u'ASHOT', u'4TEAM', u'WA_RS', u'GHATF', u'AKAI2', u'BCGT', u'FOA30', u'XSKZX', u'ARLI', u'GG333', u'XMOD', u'BOIS', u'BON2', u'XZ', u'MKB1', u'RC_67', u'7BAT', u'RUS_G', u'-HL-', u'-CRAB', u'YOUON', u'_M_S_', u'_161_', u'RTMR', u'SHIPE', u'BELAB', u'GANS0', u'HIHIH', u'IMAD', u'ATAR', u'73RU', u'EXZE', u'BFOX', u'XBOCT', u'DOFR', u'ZPVBP', u'TRF12', u'WOT_0', u'7VS0', u'CALL', u'RUG', u'008_2', u'AB_SD', u'ILA', u'NEWRA', u'S_A_P', u'_RATS', u'SZS', u'GOOD7', u'JS', u'UCXO', u'RBB', u'T1T2', u'1092', u'_I_', u'GUNBR', u'N_NOV', u'SW2', u'A__S', u'HOAX', u'DAZOR', u'T126', u'HOVOD', u'KPI', u'_RUV_', u'VPERE', u'7BSSS', u'RNE', u'92992', u'06660', u'WOT_I', u'CPC', u'O4K0', u'IIBAT', u'1TER', u'KGB7', u'RK120', u'VEBLO', u'BOSK', u'BOGI2', u'KAUFA', u'E_B', u'K9A5', u'BU03', u'TRVP', u'PROFG', u'VFGFL', u'V34', u'THT', u'ATU', u'OPG1_', u'R-13', u'_BDGI', u'RUD', u'WWRD', u'HIGH', u'-W-K-', u'NWB', u'O10', u'UL', u'REALM', u'BEK_1', u'INC_W', u'WOHT', u'TEN1_', u'96REG', u'_OXO_', u'BEAR_', u'OLEG', u'HSBT_', u'L_W_T', u'WERST', u'KML4', u'VMRK', u'TAB0R', u'CYS-A', u'026R', u'CCCCR', u'G_V_M', u'BOM1', u'ACI', u'BYAKA', u'PCG', u'LVLUP', u'T-R-F', u'_GOD_', u'BNG', u'YOUDE', u'MUMU_', u'DART', u'TAR86', u'TURS', u'1POLK', u'WECER', u'75RUS', u'SRTM', u'LEG77', u'2CCCP', u'NO-2', u'XAC', u'RUWOT', u'_PG_', u'_MH', u'BUGON', u'BY777', u'NFF88', u'ONE82', u'BROZ', u'TRPS', u'POKOR', u'RTU2', u'XONE', u'TAVRI', u'XOPB', u'VOLK2', u'-KAZ-', u'BBRUS', u'D-E-M', u'N-F', u'D_WAR', u'A-KOL', u'OZS', u'BBF', u'GAF2', u'WORID', u'COLD5', u'GPV', u'DTA', u'DHB', u'__F__', u'YOU20', u'TRIO_', u'RIP_', u'VDV96', u'SHK18', u'RU58', u'BPAN2', u'WS', u'ZLUKI', u'UFAWG', u'AUNG', u'4TUNE', u'028RU', u'SJEEE', u'99', u'LSD19', u'LOOKF', u'TAT25', u'RUS_F', u'FBR01', u'SCHMA', u'HUNT_', u'COOH', u'CVGP', u'7KING', u'ONEV', u'_R_G_', u'BPAN1', u'1VS7', u'HHBJB', u'HHH', u'BAST', u'TTT02', u'POL3', u'_VIP', u'R_B_S', u'TLTRU', u'NORIK', u'2386', u'NANAN', u'WLEG', u'14_89', u'A_R_B', u'ABR', u'ANTUT', u'FCZEN', u'SQVE', u'GONKI', u'TOP-1', u'MVM', u'BDGR', u'V3L4G', u'SNAH', u'12990', u'PZDV2', u'GRQWS', u'_MSW_', u'SAG', u'LOLZ', u'PI-VO', u'TNKST', u'14881', u'OKSI8', u'K_19', u'T4W', u'M_Z', u'TIBRO', u'MON_2', u'NORTH', u'PZ_AT', u'SIBVO', u'76176', u'RUS96', u'O55', u'WIN16', u'VALO', u'YTUBE', u'BIMA', u'CBTM', u'MARUY', u'_DK_', u'TVVIK', u'BQQM', u'KSCKS', u'SCOR', u'KPAC', u'LONEW', u'SQ_2', u'RTM', u'VALIM', u'4ECTB', u'GDDHD', u'TUCWE', u'TEG--', u'GITS', u'WERRT', u'GGOK', u'37416', u'-ZH-', u'KHG', u'GEWOT', u'Z_R_N', u'LOL3', u'4815', u'ST1_', u'TREV', u'MEMOR', u'BUNKE', u'D_E', u'WOTVP', u'XLL', u'SNOWY', u'KEKGG', u'TFTD', u'WOTKG', u'WOTWG', u'STC', u'V_I_R', u'GPY74', u'-BK-', u'PAU', u'CH-7', u'RYB', u'PRO26', u'R_K15', u'TFG', u'BEZ_B', u'SKIL', u'RUS_N', u'MS-KV', u'10326', u'XACKU', u'LGT', u'IZVOR', u'KAN', u'REJ_1', u'GTF2', u'RAPID', u'UCHA', u'OMFG', u'X88', u'-77RU', u'W-T-Z', u'UFIJX', u'10494', u'PANZE', u'WGV', u'WOT67', u'GA-ME', u'TIMT', u'VOHR', u'34568', u'S_B_K', u'KAZ16', u'NEWC', u'3434', u'PKCZ', u'ROT', u'BADIJ', u'RUTM', u'RED48', u'090', u'PT38', u'CCCP3', u'GB23', u'SDF4', u'LONE5', u'JOVE1', u'ARM_A', u'STEPW', u'_Z_', u'_SIR_', u'ZV-WT', u'KLAS2', u'DEBT', u'BILAL', u'PROTA', u'1_', u'B0B', u'RUS_2', u'HEST', u'YK1', u'OZON', u'KTI', u'TN1', u'TE3A', u'LTRA', u'WOLT', u'DENII', u'228T', u'VTR', u'SK34', u'BB', u'ST_HR', u'SAM11', u'D_O_C', u'GRAFA', u'EROHA', u'STDAN', u'XUMUK', u'GUYS1', u'MT678', u'STOL', u'VICWR', u'SVETD', u'AS06', u'IRIG', u'ELDA', u'GAGI', u'BY_5', u'UAATO', u'DIDYO', u'YARS', u'VZ_WT', u'RIGA', u'PH', u'_05RU', u'JGPZ', u'1LEG', u'JUDO', u'FBR16', u'EA', u'1941X', u'_NERV', u'PB_46', u'7PRO7', u'STGV_', u'CROCO', u'BLRMG', u'3010', u'1VOLK', u'5ARMY', u'DHDHX', u'R_T_D', u'TRAVA', u'FMLHT', u'FARSH', u'CBL', u'LOKOS', u'MOLNI', u'TIM3', u'KASTA', u'BLR1B', u'1WOT1', u'RUS_M', u'YTPM', u'WAR_Z', u'RZKD', u'MOTO2', u'GNOM', u'AIM77', u'EXPRO', u'TR01F', u'FNK', u'SPWA', u'RMSD2', u'CKK', u'_NEW', u'56RAZ', u'R_C_P', u'DAVAY', u'LTWOT', u'KAG88', u'_L0L_', u'VIY', u'GRF', u'787XX', u'STLB', u'NI_RA', u'REGE', u'BFR', u'TUJIA', u'UBOP', u'DNS-A', u'PIL', u'ELDOS', u'FVF', u'AZART', u'WOAR', u'G_G', u'MLG_G', u'13254', u'_F_P_', u'ZUBR', u'CLAN9', u'BANAN', u'5-5B', u'LIC', u'BOIH', u'ROWER', u'SS66', u'2107', u'_WHT_', u'NOMAX', u'NEYD7', u'PRS', u'WEWEW', u'OFTAN', u'BES95', u'RFGK', u'NAK2', u'OLD_F', u'CAN', u'_KT_', u'KEKS', u'OPOLO', u'WOL_2', u'BLAT', u'089-3', u'VTBBY', u'VALLI', u'VAYNA', u'L_VND', u'ONE73', u'RU_59', u'VGK', u'PROG_', u'XASKU', u'MAZER', u'LEX01', u'R-O-A', u'BASTA', u'ISY', u'DKD', u'W_A_L', u'MCPE_', u'007_6', u'WOT-I', u'FULL1', u'SRS', u'BFDT', u'UA-RU', u'SKIN', u'SEMKI', u'Y-OLA', u'CKILY', u'MBA', u'PISIA', u'064RU', u'777LE', u'CU-55', u'PDJ12', u'ORW72', u'SLRUS', u'R0A', u'IF-16', u'HAMAD', u'AS-TA', u'QAZSW', u'KDMD', u'2006B', u'MFK', u'NWT', u'V1C', u'WWWR', u'WTTH', u'SV-', u'81LG', u'RO', u'R1SK', u'BF_UA', u'PAXA2', u'1GTA', u'TMI', u'ZONE', u'LAW', u'N_3', u'SM67', u'NO_RD', u'I_WOT', u'-S_G-', u'ROME3', u'CTAJI', u'FSO5', u'APKO', u'ELIST', u'HOIN', u'TBN', u'WELCK', u'TR-L', u'GEC', u'MARI_', u'3MSV', u'LOCO', u'GATLI', u'FSB05', u'REG09', u'RU-7', u'WTFAK', u'CLOUN', u'AKAY', u'FOT_', u'H_F_S', u'NAHU', u'SIF3', u'L_UP', u'ADIK1', u'-E-', u'-DEAD', u'0808', u'LMC', u'BATE', u'GSF_1', u'ONE57', u'GGLOL', u'GRAB', u'DBN19']
clans_db_name = "clans_data"
clans_db_temp_name = clans_db_name + "_temp"


def init_db():
    for tbl_name in ["history", "last", "current"]:
        cmd = 'create table if not exists ' + tbl_name + " (uid int(10), name char(20), clan_id int(10), clan_tag char(6), " \
                                           "dmg int(10), frags int(7), date DATETIME);"
        curs_db.execute(cmd)
    cmd = 'create table ' + clans_db_name + ' (clan_id int(10), clan_tag char(6), dmg int(11));'
    curs_db.execute(cmd)
    cmd = 'create table ' + clans_db_temp_name + ' (clan_id int(10), clan_tag char(6), dmg int(11));'
    curs_db.execute(cmd)
    conn_db.commit()




def init_clans_db():
    for clan_tag in clans:
        clan_id = wg_api.get_clan_id_by_tag(clan_tag)
        cmd = 'insert into ' + clans_db_name + " values (?,?,?);"
        curs_db.execute(cmd, (clan_id, clan_tag, 0))
    conn_db.commit()


def drop_db(tbl_name):
    try:
        curs_db.execute('drop table ' + tbl_name)
    except sqlite3.OperationalError:
        pass


def clean_db(tbl_name):
    try:
        curs_db.execute("delete from " + tbl_name)
    except sqlite3.OperationalError:
        pass


def get_clan_tag_from_db(clan_id):
    assert str(clan_id).isdigit()
    cmd = 'select clan_tag from  ' + clans_db_name + ' where clan_id like ' + str(clan_id) +" ;"
    curs_db.execute(cmd)
    try:
        result = curs_db.fetchone()[0]
    except TypeError:
        logging.error("Problem with clan id: " + str(clan_id))
        raise
    return result


def get_clan_id_from_db(clan_tag):
    assert type(clan_tag) == str or type(clan_tag) == unicode
    cmd = 'select clan_id from  ' + clans_db_name + ' where clan_tag like "' + str(clan_tag) +'" ;'
    curs_db.execute(cmd)
    try:
        result = curs_db.fetchone()[0]
    except TypeError:
        logging.error("Problem with clan tag: " + str(clan_tag))
        raise
    return result


def save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, tbl_name):
    cmd = 'insert into ' + tbl_name + ' values (?,?,?,?,?,?,?);'
    curs_db.execute(cmd, (user_id, username, clan_id, clan_tag, dmg, frags, dt))


def collect_data_for_clan_members(clan_id):
    # if not str(clan_id).isdigit():
    clan_id = get_clan_id_from_db(clan_id)
    data = wg_api.get_data_for_all_user_from_clans([clan_id])
    dt = datetime.datetime.now()
    clan_tag = get_clan_tag_from_db(clan_id)

    for user_id, user_data in data.items():
        username = user_data[0]
        dmg = user_data[1]
        frags = user_data[2]
        save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, "history")
        save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, "current")
    conn_db.commit()


def shift_data_from_db_to_db(src_tbl, dst_tbl):
    clean_db(dst_tbl)
    cmd = "insert into " + dst_tbl +" select * from " + src_tbl +";"
    curs_db.execute(cmd)
    clean_db(src_tbl)


def collect_data_for_all_clans(clans_list):
    shift_data_from_db_to_db("current", "last")
    clean_db(clans_db_temp_name)

    for clan_tag in clans_list:
        collect_data_for_clan_members(clan_tag)


    cmd = "insert into " + clans_db_temp_name + " select cd.clan_id,cd.clan_tag,cd.dmg+dmg_diff from " + clans_db_name\
          +" as cd join (select current.clan_id,sum(current.dmg-last.dmg) as dmg_diff from current inner join last " \
           "using(uid) group by current.clan_id) using(clan_id);"
    curs_db.execute(cmd)
    shift_data_from_db_to_db(clans_db_temp_name, clans_db_name)
    conn_db.commit()


def get_clans_data_from_db():
    cmd = "select clan_tag,dmg from " + clans_db_name + " order by dmg DESC;"
    curs_db.execute(cmd)
    fetched = curs_db.fetchall()
    return [x[0] for x in fetched],[x[1] for x in fetched]


def get_distance_between_clan_and_top(clan_tag="XG"):
    cmd = "select dmg-(select max(dmg) from clans_data where clan_tag not like '" + clan_tag +\
          "') from clans_data where clan_tag like '" + clan_tag + "';"
    curs_db.execute(cmd)
    result = int(curs_db.fetchone()[0])
    result_E100 = abs(result) / 2300
    if abs(result) > 500000:
        result = divmod(result, 100000)[0]
        result = str(float(result)/10) + "M"
    elif abs(result) > 800:
        result = divmod(result,  100)[0]
        result = str(float(result)/10) + "K"
    return [result, result_E100]


if __name__ == "__main__":
    # drop_db("last")
    # drop_db("history")
    # drop_db("current")
    # drop_db(clans_db_name)
    # drop_db(clans_db_temp_name)
    # init_db()
    # init_clans_db()
    # collect_data_for_all_clans(clans)
    print get_distance_between_clan_and_top()