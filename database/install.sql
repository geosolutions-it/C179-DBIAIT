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
	PRO_COM 	VARCHAR(8),
	DENOM 		VARCHAR(100),
	POP_RES 	INTEGER,
	anno 		INTEGER,
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
	ae_tot	double precision
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
	protezione_catodica	BIT(1),
	note				VARCHAR(255)
);
SELECT AddGeometryColumn ('dbiait_analysis','addut_tronchi','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_COND_ALTRO;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_COND_ALTRO(
	idgis			VARCHAR(32),
	pr_min			double precision,
	pr_avg			double precision,
	pr_max			double precision,
	rip_rete		double precision,
	rip_alla		double precision,
	lu_allacci_ril	double precision,
	lu_allacci_sim	double precision,
	nr_allacci_ril	INTEGER,
	nr_allacci_sim	INTEGER
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_SHAPE;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_SHAPE(
	ids_codice	VARCHAR(32),
	comune_nom	VARCHAR(100),
	id_comune	INTEGER,
	ids_codi_1	VARCHAR(16),
	id_materia	INTEGER,
	idx_materi	VARCHAR(2),
	diametro	INTEGER,
	idx_diamet	VARCHAR(2),
	anno		INTEGER,
	idx_anno	VARCHAR(2),
	lunghez_1	double precision,
	idx_lunghe	VARCHAR(2),
	id_conserv	INTEGER,
	TIPO_RETE 	VARCHAR(100),
	TIPO_ACQUA 	VARCHAR(100),
	FUNZIONA_G 	VARCHAR(1),
	COPERTURA 	VARCHAR(100),
	PROFONDITA 	double precision,
	IDX_PROFON 	VARCHAR(2), 
	GESTIONE_P 	BIT(1), 
	ID_TIPO_TE 	VARCHAR(2), 
	PRESS_MED_ 	double precision, 
	PROTEZIONE 	BIT(1), 
	ALLACCI 	INTEGER, 
	LUNGHEZZA_ 	double precision, 
	RIPARAZION 	INTEGER, 
	RIPARAZI_1 	INTEGER, 
	UTENZE_MIS 	INTEGER, 
	ID_OPERA_S 	VARCHAR(2)
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
	id_refluo_trasportato 	VARCHAR(5),  
	LUNGHEZZA 				double precision,  
	idx_MATERIALE 			VARCHAR(5), 
	idx_DIAMETRO 			VARCHAR(5),  
	idx_ANNO 				VARCHAR(5),  
	idx_LUNGHEZZA 			VARCHAR(5),  
	funziona_gravita 		BIT(1), 
	depurazione 			BIT(1),  
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
	note 					VARCHAR(255) 
);
SELECT AddGeometryColumn ('dbiait_analysis','collett_tronchi','geom', 25832, 'MULTILINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_COND_ALTRO;
CREATE TABLE DBIAIT_ANALYSIS.FGN_COND_ALTRO(
	idgis		VARCHAR(32),
	rip_rete 	double precision,  
	rip_alla 	double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_SHAPE;
CREATE TABLE DBIAIT_ANALYSIS.FGN_SHAPE(
	ids_codice	VARCHAR(32),
	comune_nom	VARCHAR(100),
	id_comune	INTEGER,
	ids_codi_1	VARCHAR(16),
	id_materia	INTEGER,
	idx_materi	VARCHAR(2),
	sezione		VARCHAR(32),
	diametro	INTEGER,
	idx_diamet	VARCHAR(2),
	anno		INTEGER,
	idx_anno	VARCHAR(2),
	lunghez_1	double precision,
	idx_lunghe	VARCHAR(2),
	id_conserv	INTEGER,
	TIPO_RETE 	VARCHAR(100),
	id_refluo	INTEGER,
	FUNZIONA_G 	VARCHAR(1),
	recapito 	VARCHAR(100),
	COPERTURA 	VARCHAR(100),
	prof_inizi 	double precision,
	prof_final 	double precision,
	IDX_PROFON 	VARCHAR(2), 
	ALLACCI 	INTEGER, 
	ALLACCI_IN 	INTEGER,
	LUNGHEZZA_ 	double precision, 
	RIPARAZION 	INTEGER, 
	RIPARAZI_1 	INTEGER, 
	ID_OPERA_S 	VARCHAR(2)
);
SELECT AddGeometryColumn ('dbiait_analysis','fgn_shape','geom', 25832, 'LINESTRING',2);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_LUNGHEZZA_RETE;
CREATE TABLE DBIAIT_ANALYSIS.FGN_LUNGHEZZA_RETE(
	idgis			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	lunghezza 		double precision,
	lunghezza_dep 	double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.ACQ_ALLACCIO;
CREATE TABLE DBIAIT_ANALYSIS.ACQ_ALLACCIO(
	idgis			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	nr_allacci 		INTEGER,
	lung_alla 		double precision,
	nr_allacci_ril 	INTEGER,
	lung_alla_ril	double precision
);

--
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_ALLACCIO;
CREATE TABLE DBIAIT_ANALYSIS.FGN_ALLACCIO(
	idgis			VARCHAR(32),
	codice_ato		VARCHAR(32),
	tipo_infr		VARCHAR(100),
	nr_allacci_c 	INTEGER,
	lung_alla_c		double precision,
	nr_allacci_i 	INTEGER,
	lung_alla_i		double precision,
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
DROP TABLE IF EXISTS DBIAIT_ANALYSIS.FGN_REL_PROD_IMM;
CREATE TABLE DBIAIT_ANALYSIS.FGN_REL_PROD_IMM(
	id_produttivo		VARCHAR(32),
	id_immissione		VARCHAR(32),
	id_sist_fogn		VARCHAR(32)
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
	IDX_ANNO_INSTAL VARCHAR(2),
	IDX_ANNO_RISTR 	VARCHAR(2),
	IDX_POTENZA 	VARCHAR(2),	
	IDX_PORTATA 	VARCHAR(2),
	IDX_PREVALENZA 	VARCHAR(2)
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
	IDX_ANNO_INSTAL VARCHAR(2),
	IDX_ANNO_RISTR 	VARCHAR(2),
	IDX_POTENZA 	VARCHAR(2),	
	IDX_PORTATA 	VARCHAR(2),
	IDX_PREVALENZA 	VARCHAR(2)
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
	IDX_ANNO_INSTAL VARCHAR(2),
	IDX_ANNO_RISTR 	VARCHAR(2),
	IDX_POTENZA 	VARCHAR(2),	
	IDX_PORTATA 	VARCHAR(2),
	IDX_PREVALENZA 	VARCHAR(2)
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
	IDX_ANNO_INSTAL VARCHAR(2),
	IDX_ANNO_RISTR 	VARCHAR(2),
	IDX_POTENZA 	VARCHAR(2),	
	IDX_PORTATA 	VARCHAR(2),
	IDX_PREVALENZA 	VARCHAR(2)
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
    objectid bigint,
    cod_istat double precision,
    cod_reg double precision,
    cod_pro double precision,
    pro_com double precision,
    loc2011 double precision,
    loc double precision,
    tipo_loc double precision,
    denominazi character varying(100),
    altitudine character varying(10),
    centro_cl bigint,
    popres double precision,
    maschi double precision,
    famiglie double precision,
    abitazioni double precision,
    edifici double precision,
    d_ambito character varying(3),
    idloc character varying(50),
    shape_leng double precision,
    shape_area double precision
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
    shape_area double precision			--
);
SELECT AddGeometryColumn ('dbiait_analysis', 'confine_comunale', 'geom', 25832, 'MULTIPOLYGON', 2);

