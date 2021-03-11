CREATE SCHEMA DBIAIT_SYSTEM;
CREATE SCHEMA DBIAIT_ANALYSIS;
CREATE SCHEMA DBIAIT_FREEZE;

-- DOMAINS TABLE
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ALL_DOMAINS;
CREATE TABLE DBIAIT_ANALYSIS.ALL_DOMAINS(
    dominio_gis 	VARCHAR(50) NOT NULL,
    valore_gis 		VARCHAR(100) NOT NULL,
    descrizione_gis VARCHAR(255),
    dominio_netsic 	VARCHAR(50),
    valore_netsic 	VARCHAR(100),
	PRIMARY KEY(dominio_gis, valore_gis)
);

-- SUPPORT TABLES --
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.LOG_STANDALONE;
CREATE TABLE DBIAIT_ANALYSIS.LOG_STANDALONE (
	id 			VARCHAR(32) NOT NULL,
	alg_name	VARCHAR(50) NOT NULL,
	description	VARCHAR(500) NOT NULL
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POP_RES_LOC;
CREATE TABLE DBIAIT_ANALYSIS.POP_RES_LOC (
	pro_com 	VARCHAR(8) NOT NULL,
	id_localita_istat VARCHAR(20) NOT NULL,
	anno_rif INTEGER NOT NULL,
	data_rif DATE,
	popres	INTEGER NOT NULL,
	PRIMARY KEY(id_localita_istat)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.DISTRIB_LOC_SERV;
CREATE TABLE DBIAIT_ANALYSIS.DISTRIB_LOC_SERV (
	codice_opera VARCHAR(32) NOT NULL,				
	id_localita_istat VARCHAR(16) NOT NULL,
	perc_popsrv	double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_localita_istat)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POP_RES_COMUNE;
CREATE TABLE DBIAIT_ANALYSIS.POP_RES_COMUNE (
	OID			SERIAL,
	PRO_COM 	VARCHAR(8),
	DENOM 		VARCHAR(100),
	POP_RES 	INTEGER,
	anno_rif 	INTEGER,
	data_rif 	DATE,
	D_AMBITO 	VARCHAR(8),
	perc_acq 	double precision,
	pop_ser_acq INTEGER,
	perc_fgn 	double precision,
	pop_ser_fgn INTEGER,
	perc_dep 	double precision,
	pop_ser_dep INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.DISTRIB_COM_SERV;
CREATE TABLE DBIAIT_ANALYSIS.DISTRIB_COM_SERV(
	codice_opera 		VARCHAR(32) NOT NULL,				
	id_comune_istat 	VARCHAR(8) NOT NULL,
	perc_popsrv			double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_comune_istat)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FOGNAT_LOC_SERV;
CREATE TABLE DBIAIT_ANALYSIS.FOGNAT_LOC_SERV(
	codice_opera 		VARCHAR(32) NOT NULL,				
	id_localita_istat 	VARCHAR(16) NOT NULL,
	perc_popsrv			double precision NOT NULL DEFAULT 0.0,
	perc_popdep			double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_localita_istat)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FOGNAT_COM_SERV;
CREATE TABLE DBIAIT_ANALYSIS.FOGNAT_COM_SERV(
	codice_opera 		VARCHAR(32) NOT NULL,				
	id_comune_istat 	VARCHAR(4) NOT NULL,
	perc_popsrv			double precision NOT NULL DEFAULT 0.0,
	perc_popdep			double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_comune_istat)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.UTENZA_SAP;
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SAP(
	impianto 				VARCHAR(32),		
	ID_UBIC_CONTATORE		VARCHAR(32),	
	CATTARIFFA				VARCHAR(32),
	ESENTE_FOG				INTEGER,
	ESENTE_DEP				INTEGER,
	TIPO_USO				VARCHAR(50),
	NR_CONTAT_DIAM_MIN		INTEGER,
	NR_CONTAT				INTEGER,
	VOL_ACQ_FATT			double precision,
	VOL_ACQ_ERO				double precision,
	VOL_FGN_FATT			double precision,
	VOL_FGN_ERO				double precision,
	VOL_DEP_FATT			double precision,
	VOL_DEP_ERO				double precision,
	DT_RIF_VOL_FATT			DATE,
	DT_RIF_VOL_ERO			DATE,
	ANNO_RIF				INTEGER,
	U_AB					INTEGER,
	DEFALCO					VARCHAR(2)
);

-------------------------------------------------
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.UTENZA_SERVIZIO;
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SERVIZIO(
	impianto 				VARCHAR(32),	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	ids_codice_orig_ACQ		VARCHAR(32),
	id_localita_istat		VARCHAR(32),
	ids_codice_orig_FGN		VARCHAR(32),
	ids_codice_orig_DEP_SCA	VARCHAR(32)	
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.UTENZA_SERVIZIO_ACQ;
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SERVIZIO_ACQ(
	impianto 				VARCHAR(32),	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	codice					VARCHAR(32)	
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.UTENZA_SERVIZIO_LOC;
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SERVIZIO_LOC(
	impianto 				VARCHAR(32),	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	codice					VARCHAR(32)	
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.UTENZA_SERVIZIO_FGN;
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SERVIZIO_FGN(
	impianto 				VARCHAR(32),	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	codice					VARCHAR(32)	
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.UTENZA_SERVIZIO_BAC;
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SERVIZIO_BAC(
	impianto 				VARCHAR(32),	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	codice					VARCHAR(32)	
);
-------------------------------------------------

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ABITANTI_TRATTATI;
CREATE TABLE DBIAIT_ANALYSIS.ABITANTI_TRATTATI(
	idgis	VARCHAR(32),
	codice	VARCHAR(32),
	denom	VARCHAR(100),
	vol_civ	double precision,
	vol_ind	double precision,
	anno	INTEGER,
	ae_civ	double precision,
	ae_ind	double precision,	
	ae_tot	double precision,
	tipo 	VARCHAR(3)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.DISTRIB_TRONCHI;
CREATE TABLE DBIAIT_ANALYSIS.DISTRIB_TRONCHI(
	codice_ato			VARCHAR(32),
	idgis				VARCHAR(32),
	idgis_rete			VARCHAR(32),
	id_tipo_telecon		INTEGER,
	id_materiale		VARCHAR(5),
	id_conservazione	VARCHAR(5),
	diametro			INTEGER,
	anno				INTEGER,
	lunghezza			double precision,
	idx_materiale		VARCHAR(5),
	idx_diametro		VARCHAR(5),
	idx_anno			VARCHAR(5),
	idx_lunghezza		VARCHAR(5),
	pressione			BIT(1),
	note				VARCHAR(255)
);
SELECT AddGeometryColumn ('dbiait_analysis','distrib_tronchi','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ADDUT_TRONCHI;
CREATE TABLE DBIAIT_ANALYSIS.ADDUT_TRONCHI(
	codice_ato			VARCHAR(32),
	idgis				VARCHAR(32),
	idgis_rete			VARCHAR(32),
	id_tipo_telecon		INTEGER,
	id_materiale		VARCHAR(5),
	id_conservazione	VARCHAR(5),
	diametro			INTEGER,
	anno				INTEGER,
	lunghezza			double precision,
	idx_materiale		VARCHAR(5),
	idx_diametro		VARCHAR(5),
	idx_anno			VARCHAR(5),
	idx_lunghezza		VARCHAR(5),
	pressione			BIT(1),
	protezione_catodica	BIT(1),
	note				VARCHAR(255)
);
SELECT AddGeometryColumn ('dbiait_analysis','addut_tronchi','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_COND_ALTRO;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_COND_ALTRO(
	idgis			VARCHAR(32),
	id_rete			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	lu_allacci_ril	double precision,
	lu_allacci_sim	double precision,
	nr_allacci_ril	INTEGER,
	nr_allacci_sim	INTEGER
);
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_COND_EXT;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_COND_EXT(
	idgis			VARCHAR(32),
	pr_min			double precision,
	pr_avg			double precision,
	pr_max			double precision,
	rip_rete		double precision,
	rip_alla		double precision,
	PRIMARY KEY(idgis)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_SHAPE;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_SHAPE(
	ids_codice	VARCHAR(32),
	comune_nom	VARCHAR(100),
	id_comune_	INTEGER,
	ids_codi_1	VARCHAR(32),
	id_materia	VARCHAR(4),
	idx_materi	VARCHAR(4),
	diametro	INTEGER,
	idx_diamet	VARCHAR(4),
	anno		INTEGER,
	idx_anno	VARCHAR(4),
	lunghez_1	double precision,
	idx_lunghe	VARCHAR(4),
	id_conserv	VARCHAR(4),
	TIPO_RETE 	VARCHAR(100),
	TIPO_ACQUA 	VARCHAR(100),
	FUNZIONA_G 	VARCHAR(1),
	COPERTURA 	VARCHAR(100),
	PROFONDITA 	double precision,
	IDX_PROFON 	VARCHAR(4), 
	GESTIONE_P 	BIT(1), 
	ID_TIPO_TE 	VARCHAR(4), 
	PRESS_MED_ 	double precision, 
	PROTEZIONE 	BIT(1), 
	ALLACCI 	INTEGER, 
	LUNGHEZZA_ 	double precision, 
	RIPARAZION 	INTEGER, 
	RIPARAZI_1 	INTEGER, 
	UTENZE_MIS 	INTEGER, 
	ID_OPERA_S 	VARCHAR(4)
);
SELECT AddGeometryColumn ('dbiait_analysis','acq_shape','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_LUNGHEZZA_RETE;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_LUNGHEZZA_RETE(
	idgis			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	lunghezza 		double precision,
	lunghezza_tlc 	double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FOGNAT_TRONCHI;
CREATE TABLE DBIAIT_ANALYSIS.FOGNAT_TRONCHI(
	codice_ato				VARCHAR(32),
	idgis					VARCHAR(32),
	idgis_rete				VARCHAR(32),
	recapito				VARCHAR(255),
	id_materiale 			VARCHAR(5),  
	id_conservazione 		VARCHAR(5),  
	DIAMETRO 				VARCHAR(50),  
	ANNO 					INTEGER, 
	funziona_gravita 		BIT(1), 
	LUNGHEZZA 				double precision,  
	idx_MATERIALE 			VARCHAR(5), 
	idx_DIAMETRO 			VARCHAR(5),  
	idx_ANNO 				VARCHAR(5),  
	idx_LUNGHEZZA 			VARCHAR(5),  
	depurazione 			BIT(1),  
	id_refluo_trasportato 	VARCHAR(5), 
	note 					VARCHAR(255) 
);
SELECT AddGeometryColumn ('dbiait_analysis','fognat_tronchi','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.COLLETT_TRONCHI;
CREATE TABLE DBIAIT_ANALYSIS.COLLETT_TRONCHI(
	codice_ato				VARCHAR(32),
	idgis					VARCHAR(32),
	idgis_rete				VARCHAR(32),
	recapito				VARCHAR(255),
	id_materiale 			VARCHAR(5),  
	id_conservazione 		VARCHAR(5),  
	DIAMETRO 				VARCHAR(50),  
	ANNO 					INTEGER, 
	funziona_gravita 		BIT(1), 
	LUNGHEZZA 				double precision,  
	idx_MATERIALE 			VARCHAR(5), 
	idx_DIAMETRO 			VARCHAR(5),  
	idx_ANNO 				VARCHAR(5),  
	idx_LUNGHEZZA 			VARCHAR(5),  
	depurazione 			BIT(1),  
	id_refluo_trasportato 	VARCHAR(5), 
	note 					VARCHAR(255) 
);
SELECT AddGeometryColumn ('dbiait_analysis','collett_tronchi','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_COND_ALTRO;
CREATE TABLE DBIAIT_ANALYSIS.FGN_COND_ALTRO(
	idgis				VARCHAR(32),
	id_rete				VARCHAR(32),
	codice_ato			VARCHAR(32),
	tipo_infr			VARCHAR(100),
	lu_allacci_c		double precision,
	lu_allacci_c_ril	double precision,
	lu_allacci_i		double precision,
	lu_allacci_i_ril	double precision,
	nr_allacci_c		INTEGER,
	nr_allacci_c_ril	INTEGER,
	nr_allacci_i		INTEGER,
	nr_allacci_i_ril	INTEGER
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_COND_EXT;
CREATE TABLE DBIAIT_ANALYSIS.FGN_COND_EXT(
	idgis				VARCHAR(32),
	rip_rete 			double precision,  
	rip_alla 			double precision
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_SHAPE;
CREATE TABLE DBIAIT_ANALYSIS.FGN_SHAPE(
	ids_codice	VARCHAR(32),
	comune_nom	VARCHAR(100),
	id_comune_  INTEGER,
	ids_codi_1	VARCHAR(32),
	id_materia	INTEGER,
	idx_materi	VARCHAR(4),
	sezione		VARCHAR(32),
	diametro	INTEGER,
	idx_diamet	VARCHAR(4),
	anno		INTEGER,
	idx_anno	VARCHAR(4),
	lunghez_1	double precision,
	idx_lunghe	VARCHAR(4),
	id_conserv	INTEGER,
	TIPO_RETE 	VARCHAR(100),
	id_refluo_	INTEGER,
	FUNZIONA_G 	VARCHAR(1),
	recapito 	VARCHAR(100),
	COPERTURA 	VARCHAR(100),
	prof_inizi 	double precision,
	prof_final 	double precision,
	IDX_PROFON 	VARCHAR(4), 
	ALLACCI 	INTEGER, 
	ALLACCI_IN 	INTEGER,
	LUNGHEZZA_ 	double precision, 
	RIPARAZION 	INTEGER, 
	RIPARAZI_1 	INTEGER, 
	ID_OPERA_S 	VARCHAR(4)
);
SELECT AddGeometryColumn ('dbiait_analysis','fgn_shape','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_LUNGHEZZA_RETE;
CREATE TABLE DBIAIT_ANALYSIS.FGN_LUNGHEZZA_RETE(
	idgis			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	lunghezza 		double precision,
	lunghezza_dep 	double precision,
	id_refluo_trasportato INTEGER,
	lung_rete_mista 	double precision,
	lung_rete_nera 	double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_LUNGHEZZA_ALLACCI;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_LUNGHEZZA_ALLACCI(
	idgis			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	nr_allacci 		INTEGER,
	lung_alla 		double precision,
	nr_allacci_ril 	INTEGER,
	lung_alla_ril	double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.fgn_lunghezza_allacci;
CREATE TABLE DBIAIT_ANALYSIS.fgn_lunghezza_allacci(
	idgis				VARCHAR(32),
	codice_ato			VARCHAR(32),
	tipo_infr			VARCHAR(100),
	nr_allacci_c 		INTEGER,
	lung_alla_c			double precision,
	nr_allacci_i 		INTEGER,
	lung_alla_i			double precision,
	nr_allacci_c_ril 	INTEGER,
	lung_alla_c_ril		double precision,
	nr_allacci_i_ril 	INTEGER,
	lung_alla_i_ril		double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.REL_PROD_CONT;
CREATE TABLE DBIAIT_ANALYSIS.REL_PROD_CONT(
	idgis_contatore		VARCHAR(32),
	idgis_produttivo	VARCHAR(32),
	sn_allac_fgn_335	VARCHAR(2),
	note 				VARCHAR(255) 
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.A_REL_PROD_CONT;
CREATE TABLE DBIAIT_ANALYSIS.A_REL_PROD_CONT(
	idgis_contatore		VARCHAR(32),
	idgis_produttivo	VARCHAR(32),
	sn_allac_fgn_335	VARCHAR(2),
	note 				VARCHAR(255) 
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.A_FGN_REL_PROD_IMM;
CREATE TABLE DBIAIT_ANALYSIS.A_FGN_REL_PROD_IMM(
	id_produttivo		VARCHAR(32),
	id_immissione		VARCHAR(32),
	id_sist_fogn		VARCHAR(32),
	primary key (id_produttivo, id_immissione)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ARCHIVIO_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.ARCHIVIO_POMPE(
	ID_OGGETTO 		VARCHAR(32), 
	CODICE_ATO 		VARCHAR(32), 
	D_STATO_CONS 	VARCHAR(3), 
	ANNO_INSTAL 	INTEGER, 
	ANNO_RISTR 		INTEGER, 
	POTENZA 		double precision, 
	PORTATA 		double precision, 
	PREVALENZA 		double precision, 
	SN_RISERVA 		VARCHAR(3), 
	A_ANNO_INSTAL 	VARCHAR(3), 
	A_ANNO_RISTR 	VARCHAR(3), 
	A_POTENZA 		VARCHAR(3), 
	A_PORTATA 		VARCHAR(3), 
	A_PREVALENZA 	VARCHAR(3), 
	MARCA 			VARCHAR(20), 
	MODELLO 		VARCHAR(20), 
	MATRICOLA 		VARCHAR(20), 
	TIPO_OGGETTO 	VARCHAR(50), 
	ANNOTAZIONE 	VARCHAR(150) 
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POZZI_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.POZZI_POMPE(
	CODICE_ATO 		VARCHAR(32), 
	D_STATO_CONS 	VARCHAR(4), 
	ANNO_INSTAL 	INTEGER,
	ANNO_RISTR 		INTEGER,
	POTENZA 		double precision, 
	PORTATA 		double precision, 
	PREVALENZA 		double precision, 
	SN_RISERVA 		BIT(1),
	IDX_ANNO_INSTAL VARCHAR(3),
	IDX_ANNO_RISTR 	VARCHAR(3),
	IDX_POTENZA 	VARCHAR(3),	
	IDX_PORTATA 	VARCHAR(3),
	IDX_PREVALENZA 	VARCHAR(3)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POTAB_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.POTAB_POMPE(
	CODICE_ATO 		VARCHAR(32), 
	D_STATO_CONS 	VARCHAR(4), 
	ANNO_INSTAL 	INTEGER,
	ANNO_RISTR 		INTEGER,
	POTENZA 		double precision, 
	PORTATA 		double precision, 
	PREVALENZA 		double precision, 
	SN_RISERVA 		BIT(1),
	IDX_ANNO_INSTAL VARCHAR(3),
	IDX_ANNO_RISTR 	VARCHAR(3),
	IDX_POTENZA 	VARCHAR(3),	
	IDX_PORTATA 	VARCHAR(3),
	IDX_PREVALENZA 	VARCHAR(3)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POMPAGGI_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.POMPAGGI_POMPE(
	CODICE_ATO 		VARCHAR(32), 
	D_STATO_CONS 	VARCHAR(4), 
	ANNO_INSTAL 	INTEGER,
	ANNO_RISTR 		INTEGER,
	POTENZA 		double precision, 
	PORTATA 		double precision, 
	PREVALENZA 		double precision, 
	SN_RISERVA 		BIT(1),
	IDX_ANNO_INSTAL VARCHAR(3),
	IDX_ANNO_RISTR 	VARCHAR(3),
	IDX_POTENZA 	VARCHAR(3),	
	IDX_PORTATA 	VARCHAR(3),
	IDX_PREVALENZA 	VARCHAR(3)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.SOLLEV_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.SOLLEV_POMPE(
	CODICE_ATO 		VARCHAR(32), 
	D_STATO_CONS 	VARCHAR(4), 
	ANNO_INSTAL 	INTEGER,
	ANNO_RISTR 		INTEGER,
	POTENZA 		double precision, 
	PORTATA 		double precision, 
	PREVALENZA 		double precision, 
	SN_RISERVA 		BIT(1),
	IDX_ANNO_INSTAL VARCHAR(3),
	IDX_ANNO_RISTR 	VARCHAR(3),
	IDX_POTENZA 	VARCHAR(3),	
	IDX_PORTATA 	VARCHAR(3),
	IDX_PREVALENZA 	VARCHAR(3)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.DEPURATO_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.DEPURATO_POMPE(
	CODICE_ATO 		VARCHAR(32), 
	D_STATO_CONS 	VARCHAR(4), 
	ANNO_INSTAL 	INTEGER,
	ANNO_RISTR 		INTEGER,
	POTENZA 		double precision, 
	PORTATA 		double precision, 
	PREVALENZA 		double precision, 
	SN_RISERVA 		BIT(1),
	IDX_ANNO_INSTAL VARCHAR(3),
	IDX_ANNO_RISTR 	VARCHAR(3),
	IDX_POTENZA 	VARCHAR(3),	
	IDX_PORTATA 	VARCHAR(3),
	IDX_PREVALENZA 	VARCHAR(3)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.STATS_POMPE;
CREATE TABLE DBIAIT_ANALYSIS.STATS_POMPE(
	codice_ato	VARCHAR(32),
	sum_potenza double precision, 
	avg_idx_potenza VARCHAR(3)
);

------------------------- Altre tabelle ---------------------------------
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FIUMI_INPOTAB;
CREATE TABLE DBIAIT_ANALYSIS.FIUMI_INPOTAB(
	ids_codice 			VARCHAR(32), 
	ids_codice_potab 	VARCHAR(16), 
	id_gestore_potab	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FIUMI_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.FIUMI_INRETI(
	ids_codice 			VARCHAR(32), 
	ids_codice_rete 	VARCHAR(16), 
	id_gestore_rete		INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.LAGHI_INPOTAB;
CREATE TABLE DBIAIT_ANALYSIS.LAGHI_INPOTAB(
	ids_codice 			VARCHAR(32), 
	ids_codice_potab 	VARCHAR(16), 
	id_gestore_potab	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.LAGHI_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.LAGHI_INRETI(
	ids_codice 			VARCHAR(32), 
	ids_codice_rete 	VARCHAR(16), 
	id_gestore_rete		INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POZZI_INPOTAB;
CREATE TABLE DBIAIT_ANALYSIS.POZZI_INPOTAB(
	ids_codice 			VARCHAR(32), 
	ids_codice_potab 	VARCHAR(16), 
	id_gestore_potab	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.SUPPORT_POZZI_INPOTAB;
CREATE TABLE DBIAIT_ANALYSIS.SUPPORT_POZZI_INPOTAB(
	ids_codice 			VARCHAR(32), 
	volume_medio_prel 	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POZZI_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.POZZI_INRETI(
	ids_codice 			VARCHAR(32), 
	ids_codice_rete 	VARCHAR(16), 
	id_gestore_rete		INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.SORGENTI_INPOTAB;
CREATE TABLE DBIAIT_ANALYSIS.SORGENTI_INPOTAB(
	ids_codice 			VARCHAR(32), 
	ids_codice_potab 	VARCHAR(16), 
	id_gestore_potab	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.SORGENTI_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.SORGENTI_INRETI(
	ids_codice 			VARCHAR(32), 
	ids_codice_rete 	VARCHAR(16), 
	id_gestore_rete		INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POTAB_INCAPTAZ;
CREATE TABLE DBIAIT_ANALYSIS.POTAB_INCAPTAZ(
	ids_codice 				VARCHAR(32), 
	ids_codice_captazione 	VARCHAR(16), 
	id_gestore_captazione	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POTAB_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.POTAB_INRETI(
	ids_codice 		VARCHAR(32), 
	ids_codice_rete VARCHAR(16), 
	id_gestore_rete	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ADDUT_COM_SERV;
CREATE TABLE DBIAIT_ANALYSIS.ADDUT_COM_SERV(
	ids_codice 		VARCHAR(32), 
	id_comune_istat VARCHAR(8)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ADDUT_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.ADDUT_INRETI(
	ids_codice 			VARCHAR(32), 
	ids_codice_rete 	VARCHAR(16), 
	id_gestore_rete		INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACCUMULI_INADD;
CREATE TABLE DBIAIT_ANALYSIS.ACCUMULI_INADD(
	ids_codice 				VARCHAR(32), 
	ids_codice_adduzione 	VARCHAR(16), 
	id_gestore_adduzione	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACCUMULI_INRETI;
CREATE TABLE DBIAIT_ANALYSIS.ACCUMULI_INRETI(
	ids_codice 			VARCHAR(32), 
	ids_codice_rete 	VARCHAR(16), 
	id_gestore_rete		INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POMPAGGI_INPOTAB;
CREATE TABLE DBIAIT_ANALYSIS.POMPAGGI_INPOTAB(
	ids_codice 			VARCHAR(32), 
	ids_codice_potab 	VARCHAR(16), 
	id_gestore_potab	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.POMPAGGI_INSERBA;
CREATE TABLE DBIAIT_ANALYSIS.POMPAGGI_INSERBA(
	ids_codice 				VARCHAR(32), 
	ids_codice_serbatoio 	VARCHAR(16), 
	id_gestore_serbatoio	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.COLLET_COM_SERV;
CREATE TABLE DBIAIT_ANALYSIS.COLLET_COM_SERV(
	ids_codice 		VARCHAR(32), 
	id_comune_istat VARCHAR(8)
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.DEPURATO_INCOLL;
CREATE TABLE DBIAIT_ANALYSIS.DEPURATO_INCOLL(
	ids_codice 				VARCHAR(32), 
	ids_codice_collettore 	VARCHAR(16), 
	id_gestore_collettore	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.SCARICATO_INFOG;
CREATE TABLE DBIAIT_ANALYSIS.SCARICATO_INFOG(
	ids_codice 				VARCHAR(32), 
	ids_codice_fognatura 	VARCHAR(16), 
	id_gestore_fognatura	INTEGER
);

-- 
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.localita;
CREATE TABLE DBIAIT_ANALYSIS.localita(
    id_localita_istat 	VARCHAR(20),
	cod_istat 			VARCHAR(10),
    cod_reg 			VARCHAR(5),
    cod_pro 			VARCHAR(5),
    pro_com 			INTEGER,
    loc					VARCHAR(5),
    tipo_loc			VARCHAR(5),
    denominazi 			VARCHAR(100),
    altitudine 			DOUBLE PRECISION,
    centro_cl 			VARCHAR(5),
    popres 				INTEGER,
    maschi 				INTEGER,
    famiglie 			INTEGER,
    abitazioni 			INTEGER,
    edifici 			INTEGER,
    d_ambito 			VARCHAR(3),
	PRIMARY KEY (id_localita_istat)
);
SELECT AddGeometryColumn ('dbiait_analysis', 'localita', 'geom', 25832, 'MULTIPOLYGON', 2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.confine_comunale;
CREATE TABLE DBIAIT_ANALYSIS.confine_comunale
(
    cod_istat double precision,
    provincia character varying(50),	--
    pro_com_tx character varying(20),	--
    pro_com bigint,						--
    denom character varying(100),		--
    pro_com__1 character varying(50),
    shape_leng double precision,
    shape_area double precision,		
	PRIMARY KEY(pro_com)
);
SELECT AddGeometryColumn ('dbiait_analysis', 'confine_comunale', 'geom', 25832, 'MULTIPOLYGON', 2);
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.DECOD_COM;
CREATE TABLE DBIAIT_ANALYSIS.DECOD_COM(
    pro_com_acc 	INTEGER,
	denom_com_acc 	VARCHAR(100),
    pro_com 		INTEGER,
    denom_com 		VARCHAR(100),
	PRIMARY KEY (pro_com)
);
INSERT INTO DBIAIT_ANALYSIS.DECOD_COM(pro_com_acc, denom_com_acc, pro_com, denom_com) VALUES (48054, 'Barberino Tavarnelle',  48003, 'Barberino Val D''Elsa');
INSERT INTO DBIAIT_ANALYSIS.DECOD_COM(pro_com_acc, denom_com_acc, pro_com, denom_com) VALUES (48054, 'Barberino Tavarnelle',  48045, 'Tavarnelle Val di Pesa');
INSERT INTO DBIAIT_ANALYSIS.DECOD_COM(pro_com_acc, denom_com_acc, pro_com, denom_com) VALUES (47024, 'San Marcello Piteglio', 47019, 'San Marcello Pistoiese');
INSERT INTO DBIAIT_ANALYSIS.DECOD_COM(pro_com_acc, denom_com_acc, pro_com, denom_com) VALUES (47024, 'San Marcello Piteglio', 47015, 'Piteglio');
---------------------------------------------------------------------------------------------------
alter table DBIAIT_ANALYSIS.confine_comunale add constraint confine_comunale_uq UNIQUE(pro_com);
alter table DBIAIT_ANALYSIS.pop_res_comune   add constraint pop_res_comune_pk PRIMARY KEY(pro_com);
alter table DBIAIT_ANALYSIS.utenza_sap       add constraint utenza_sap_pk PRIMARY KEY(impianto);
alter table DBIAIT_ANALYSIS.acq_cond_altro   add constraint acq_cond_altro_pk PRIMARY KEY(idgis);
alter table DBIAIT_ANALYSIS.fgn_cond_altro   add constraint fgn_cond_altro_pk PRIMARY KEY(idgis);
alter table DBIAIT_ANALYSIS.archivio_pompe   add column oid serial;
alter table DBIAIT_ANALYSIS.FIUMI_INRETI     add constraint fiumi_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.LAGHI_INRETI     add constraint laghi_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.POZZI_INRETI     add constraint pozzi_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.SORGENTI_INRETI  add constraint sorgenti_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.POTAB_INRETI     add constraint potab_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.ADDUT_INRETI     add constraint addut_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.ACCUMULI_INRETI  add constraint accumuli_inreti_pk PRIMARY KEY(ids_codice,ids_codice_rete);
alter table DBIAIT_ANALYSIS.ADDUT_COM_SERV   add constraint ADDUT_COM_SERV_pk PRIMARY KEY(ids_codice,id_comune_istat);
alter table DBIAIT_ANALYSIS.COLLET_COM_SERV  add constraint COLLET_COM_SERV_pk PRIMARY KEY(ids_codice,id_comune_istat);
alter table DBIAIT_ANALYSIS.ACCUMULI_INADD   add constraint ACCUMULI_INADD_pk PRIMARY KEY(ids_codice,ids_codice_adduzione);
alter table DBIAIT_ANALYSIS.DEPURATO_INCOLL  add constraint DEPURATO_INCOLL_pk PRIMARY KEY(ids_codice,ids_codice_collettore);
alter table DBIAIT_ANALYSIS.SCARICATO_INFOG  add constraint SCARICATO_INFOG_pk PRIMARY KEY(ids_codice,ids_codice_fognatura);
alter table DBIAIT_ANALYSIS.FIUMI_INPOTAB    add constraint FIUMI_INPOTAB_pk PRIMARY KEY(ids_codice,ids_codice_potab);
alter table DBIAIT_ANALYSIS.LAGHI_INPOTAB    add constraint LAGHI_INPOTAB_pk PRIMARY KEY(ids_codice,ids_codice_potab);
alter table DBIAIT_ANALYSIS.POZZI_INPOTAB    add constraint POZZI_INPOTAB_pk PRIMARY KEY(ids_codice,ids_codice_potab);
alter table DBIAIT_ANALYSIS.SORGENTI_INPOTAB add constraint SORGENTI_INPOTAB_pk PRIMARY KEY(ids_codice,ids_codice_potab);
alter table DBIAIT_ANALYSIS.POTAB_INCAPTAZ	 add constraint POTAB_INCAPTAZ_pk PRIMARY KEY(ids_codice,ids_codice_captazione);
alter table DBIAIT_ANALYSIS.POMPAGGI_INPOTAB add constraint POMPAGGI_INPOTAB_pk PRIMARY KEY(ids_codice,ids_codice_potab);
alter table DBIAIT_ANALYSIS.POMPAGGI_INSERBA add constraint POMPAGGI_INSERBA_pk PRIMARY KEY(ids_codice,ids_codice_serbatoio);
--alter table DBIAIT_ANALYSIS.DECOD_COM	       add constraint DECOD_COM_pk PRIMARY KEY(PRO_COM_ACC,PRO_COM);
alter table DBIAIT_ANALYSIS.UTENZA_SERVIZIO	   add constraint UTENZA_SERVIZIO_pk PRIMARY KEY(ID_UBIC_CONTATORE);
alter table DBIAIT_ANALYSIS.ABITANTI_TRATTATI  add constraint ABITANTI_TRATTATI_pk PRIMARY KEY(IDGIS);
alter table DBIAIT_ANALYSIS.DISTRIB_TRONCHI	   add constraint DISTRIB_TRONCHI_pk PRIMARY KEY(IDGIS);
alter table DBIAIT_ANALYSIS.ADDUT_TRONCHI	   add constraint ADDUT_TRONCHI_pk PRIMARY KEY(IDGIS);
alter table DBIAIT_ANALYSIS.ACQ_SHAPE	       add constraint ACQ_SHAPE_pk PRIMARY KEY(ids_codi_1);
alter table DBIAIT_ANALYSIS.ACQ_LUNGHEZZA_RETE add constraint ACQ_LUNGHEZZA_RETE_pk PRIMARY KEY(idgis);
alter table DBIAIT_ANALYSIS.FOGNAT_TRONCHI	   add constraint FOGNAT_TRONCHI_pk PRIMARY KEY(idgis);
alter table DBIAIT_ANALYSIS.COLLETT_TRONCHI	   add constraint COLLETT_TRONCHI_pk PRIMARY KEY(idgis);
alter table DBIAIT_ANALYSIS.FGN_SHAPE	       add constraint FGN_SHAPE_pk PRIMARY KEY(ids_codi_1);
alter table DBIAIT_ANALYSIS.FGN_LUNGHEZZA_RETE add constraint FGN_LUNGHEZZA_RETE_pk PRIMARY KEY(idgis);
alter table DBIAIT_ANALYSIS.ACQ_LUNGHEZZA_ALLACCI	   add constraint ACQ_LUNGHEZZA_ALLACCI_pk PRIMARY KEY(IDGIS);
alter table DBIAIT_ANALYSIS.fgn_lunghezza_allacci	   add constraint fgn_lunghezza_allacci_pk PRIMARY KEY(IDGIS);
alter table DBIAIT_ANALYSIS.POMPAGGI_POMPE	   add column oid SERIAL;
alter table DBIAIT_ANALYSIS.SOLLEV_POMPE	   add column oid SERIAL;
alter table DBIAIT_ANALYSIS.DEPURATO_POMPE	   add column oid SERIAL;
alter table DBIAIT_ANALYSIS.POTAB_POMPE	       add column oid SERIAL;
alter table DBIAIT_ANALYSIS.POZZI_POMPE	       add column oid SERIAL;
alter table DBIAIT_ANALYSIS.UTENZA_SERVIZIO_ACQ	add constraint UTENZA_SERVIZIO_ACQ_pk PRIMARY KEY(ID_UBIC_CONTATORE);
alter table DBIAIT_ANALYSIS.UTENZA_SERVIZIO_LOC	add constraint UTENZA_SERVIZIO_LOC_pk PRIMARY KEY(ID_UBIC_CONTATORE);
alter table DBIAIT_ANALYSIS.UTENZA_SERVIZIO_FGN	add constraint UTENZA_SERVIZIO_FGN_pk PRIMARY KEY(ID_UBIC_CONTATORE);
alter table DBIAIT_ANALYSIS.UTENZA_SERVIZIO_BAC	add constraint UTENZA_SERVIZIO_BAC_pk PRIMARY KEY(ID_UBIC_CONTATORE);
-----------------------------------------------------------------------------------------------------------------------
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_LUNGHEZZA_ALLACCI_ID_RETE;
CREATE TABLE DBIAIT_ANALYSIS.FGN_LUNGHEZZA_ALLACCI_ID_RETE(
    id_rete     VARCHAR(32),
    lunghezza_allaccio     double precision
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_VOL_UTENZE;
CREATE TABLE DBIAIT_ANALYSIS.FGN_VOL_UTENZE(
	ids_codice_orig_fgn		VARCHAR(32),
	utenze_prod_auth	bigint,
	vol_fatturato	double precision,
	vol_utenze_auth	double precision
);
--
-- FASE ADDONS
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.TAB_ISPEZIONI;
CREATE TABLE DBIAIT_ANALYSIS.TAB_ISPEZIONI(
	odl			VARCHAR(32),
	idgis		VARCHAR(32),
	lu_mista	DOUBLE PRECISION,
	lu_nera		DOUBLE PRECISION,
	tipo		VARCHAR(20),
	dt_odl		DATE,
	tipo_odl	VARCHAR(50),
	tam			VARCHAR(10),
	primary key (odl)
);
-----

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_VOL_UTENZE;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_VOL_UTENZE(
	ids_codice_orig_acq		VARCHAR(32),
    totalCount	INTEGER,
    countDomestiche	INTEGER,
    countDomesticheResidente	INTEGER,
    countDomesticheDiamMin	INTEGER,
    countCommerciali	INTEGER,
    countPubblico	INTEGER,
    countIndustriale	INTEGER,
    countutenzeconmisuratore	INTEGER,
    sumVolAcqEro	double precision,
    sumVolAcqFatt	double precision,
    sumDomesticheVolFatt	double precision,
    sumDomesticheResidenteVolFatt	double precision,
    sumPubblicoeVolFatt	double precision,
    sumAltroVolFatt	double precision
);
--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.STATS_CLORATORE;
CREATE TABLE DBIAIT_ANALYSIS.STATS_CLORATORE(
	id_rete		VARCHAR(32),
	counter	bigint
);

-----

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_CAPT_ACCORPAM;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_CAPT_ACCORPAM(
	codice_acc		VARCHAR(32),
    denom	VARCHAR(200)
);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.area_poe;
CREATE TABLE DBIAIT_ANALYSIS.area_poe
(
    codice_schema_acq varchar(32),
    denominazione_schema_acq character varying(100),
	PRIMARY KEY(codice_schema_acq)
);
SELECT AddGeometryColumn ('dbiait_analysis', 'area_poe', 'geom', 25832, 'MULTIPOLYGON', 2);


DROP TABLE IF EXISTS DBIAIT_ANALYSIS.schema_acq;
CREATE TABLE DBIAIT_ANALYSIS.schema_acq(
    idgis       VARCHAR(32),
	codice_schema_acq		text,
    denominazione_schema_acq	text,
    primary key (idgis)
);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.acq_allaccio;
CREATE TABLE DBIAIT_ANALYSIS.acq_allaccio(
    id_cassetta       VARCHAR(32),
    id_condotta     VARCHAR(32),
    id_derivazione     VARCHAR(32),
    lungh_all     DOUBLE PRECISION,
    tipo     VARCHAR(100),
    nr_cont_cass     INTEGER

);
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ubic_contatori_cass_cont;
CREATE TABLE DBIAIT_ANALYSIS.ubic_contatori_cass_cont(
    id_ubic_contatore     VARCHAR(32),
    id_cass_cont     VARCHAR(32)
);


DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ubic_allaccio;
CREATE TABLE DBIAIT_ANALYSIS.ubic_allaccio(
    id_ubic_contatore     VARCHAR(32),
    acq_sn_alla     VARCHAR(32),
    acq_idrete     VARCHAR(32)
);


DROP TABLE IF EXISTS DBIAIT_ANALYSIS.utenza_defalco;
CREATE TABLE DBIAIT_ANALYSIS.utenza_defalco(
    imp_defalco     	bigint,
    imp_divisionale 	bigint,
    idgis_defalco   	VARCHAR(32),
    idgis_divisionale   VARCHAR(32),
    dt_iniz_val     	date,
    dt_fine_val     	date,
    annotazioni     	text,
	primary key (idgis_defalco,idgis_divisionale)
);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.support_acq_allacci;
CREATE TABLE DBIAIT_ANALYSIS.support_acq_allacci(
	id_cassetta			VARCHAR(32),
	id_condotta			VARCHAR(32),
	id_derivazione			VARCHAR(32),
	sub_funzione		INTEGER,
	nr_allacci	INTEGER,
	lung_alla	double precision,
	nr_allacci_ril	INTEGER,
	lung_alla_ril	double precision
);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.utenze_distribuzioni_adduttrici;
CREATE TABLE DBIAIT_ANALYSIS.utenze_distribuzioni_adduttrici(
	id_rete			VARCHAR(32),
	nr_utenze_dirette			INTEGER,
	nr_utenze_dir_dom_e_residente			INTEGER,
	nr_utenze_dir_residente		INTEGER,
	nr_utenze_condominiali	INTEGER,
	nr_utenze_indir_indirette 	INTEGER,
	nr_utenze_indir_domestici	INTEGER,
	nr_utenze_indir_residente	INTEGER,
	nr_utenze_misuratore integer,
	volume_erogato double precision,
	volume_fatturato double precision,
	nr_allacci integer
);


DROP TABLE IF EXISTS DBIAIT_ANALYSIS.fgn_allaccio;
CREATE TABLE DBIAIT_ANALYSIS.fgn_allaccio(
    id_fossa       VARCHAR(20),
    id_condotta     VARCHAR(20),
    id_immissione     VARCHAR(20),
    lungh_all     DOUBLE PRECISION,
    tipo     VARCHAR(10),
    industriale     VARCHAR(2)

);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.support_fgn_allacci;
CREATE TABLE DBIAIT_ANALYSIS.support_fgn_allacci(
	id_fossa_settica			VARCHAR(20),
	id_condotta			VARCHAR(20),
	id_immissione			VARCHAR(20),
	tipo			VARCHAR(20),
	lu_allacci_c        DOUBLE PRECISION,
	lu_allacci_c_ril        DOUBLE PRECISION,
	lu_allacci_i        DOUBLE PRECISION,
	lu_allacci_i_ril        DOUBLE PRECISION,
	nr_allacci_c        INTEGER,
	nr_allacci_c_ril        INTEGER,
	nr_allacci_i        INTEGER,
	nr_allacci_i_ril        INTEGER
);


DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ubic_f_allaccio;
CREATE TABLE DBIAIT_ANALYSIS.ubic_f_allaccio(
    id_ubic_contatore     VARCHAR(20),
    fgn_sn_alla     VARCHAR(2),
    fgn_idrete     VARCHAR(20)
);

-- support table
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ubic_contatori_fgn;
CREATE TABLE DBIAIT_ANALYSIS.ubic_contatori_fgn(
    id_ubic_contatore     VARCHAR(32),
    id_fossa     VARCHAR(32)
);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.utenze_fognature_collettori;
CREATE TABLE DBIAIT_ANALYSIS.utenze_fognature_collettori(
	id_rete			VARCHAR(32),
	nr_utenze_totali			INTEGER,
	nr_utenze_industriali			INTEGER,
	volume_utenze_industriali double precision,
	volume_utenze_totali double precision
);

DROP TABLE IF EXISTS DBIAIT_ANALYSIS.support_codice_capt_accorp;
CREATE TABLE DBIAIT_ANALYSIS.support_codice_capt_accorp(
	idgis			VARCHAR(32),
	codice_accorp_capt			VARCHAR(32),
	denom			VARCHAR(100)
);