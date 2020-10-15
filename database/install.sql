CREATE SCHEMA DBIAIT_SYSTEM;
CREATE SCHEMA DBIAIT_ANALYSIS;
CREATE SCHEMA DBIAIT_FREEZE;

-- DOMAINS TABLE
CREATE TABLE DBIAIT_ANALYSIS.ALL_DOMAINS(
    dominio_gis 	VARCHAR(50) NOT NULL,
    valore_gis 		VARCHAR(100) NOT NULL,
    descrizione_gis VARCHAR(255),
    dominio_netsic 	VARCHAR(50),
    valore_netsic 	VARCHAR(100),
	PRIMARY KEY(dominio_gis, valore_gis)
)

-- SUPPORT TABLES --
CREATE TABLE DBIAIT_ANALYSIS.POP_RES_LOC (
	id_localita_istat VARCHAR(16) NOT NULL,
	anno_rif INTEGER NOT NULL,
	popres	INTEGER NOT NULL,
	PRIMARY KEY(id_localita_istat)
);

CREATE TABLE DBIAIT_ANALYSIS.DISTRIB_LOC_SERV (
	codice_opera VARCHAR(32) NOT NULL,				
	id_localita_istat VARCHAR(16) NOT NULL,
	perc_popsrv	double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_localita_istat)
);

 -- DEFINZIONE DEI CAMPI???
CREATE TABLE DBIAIT_ANALYSIS.POP_RES_COMUNE (
	PRO_COM VARCHAR(5),
	DENOM VARCHAR(100),
	D_AMBITO VARCHAR(100),
);

CREATE TABLE DBIAIT_ANALYSIS.DISTRIB_COM_SERV(
	codice_opera 		VARCHAR(32) NOT NULL,				
	id_comune_istat 	VARCHAR(4) NOT NULL,
	perc_popsrv			double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_comune_istat)
);

CREATE TABLE DBIAIT_ANALYSIS.FOGNAT_LOC_SERV(
	codice_opera 		VARCHAR(32) NOT NULL,				
	id_localita_istat 	VARCHAR(16) NOT NULL,
	perc_popsrv			double precision NOT NULL DEFAULT 0.0,
	perc_popdep			double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_comune_istat)
);

CREATE TABLE DBIAIT_ANALYSIS.FOGNAT_COM_SERV(
	codice_opera 		VARCHAR(32) NOT NULL,				
	id_comune_istat 	VARCHAR(4) NOT NULL,
	perc_popsrv			double precision NOT NULL DEFAULT 0.0,
	perc_popdep			double precision NOT NULL DEFAULT 0.0,
	PRIMARY KEY(codice_opera, id_comune_istat)
);

-- Lunghezze campi?
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SAP(
	impianto 				INTEGER,		--INTERO??? (in altre tabelle e' VARCHAR!!!)	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	CATTARIFFA				VARCHAR(32),
	ESENTE_FOG				INTEGER,
	ESENTE_DEP				INTEGER,
	TIPO_USO				VARCHAR(32),
	NR_CONTAT_DIAM_MIN		INTEGER,
	NR_CONTAT				INTEGER,
	VOL_ACQ_FATT			double precision,
	VOL_ACQ_ERO				double precision,
	VOL_FGN_FATT			double precision,
	VOL_FGN_ERO				double precision,
	VOL_DEP_FATT			double precision,
	VOL_DEP_ERO				double precision,
	DT_RIF_VOL_FATT			DATE,
	DT_RIF_VOL_ERO			DATE
);

-- Lunghezze campi?
CREATE TABLE DBIAIT_ANALYSIS.UTENZA_SERVIZIO(
	impianto 				INTEGER,	
	ID_UBIC_CONTATORE		VARCHAR(32),	
	ids_codice_orig_ACQ		VARCHAR(32),
	id_localita_istat		VARCHAR(32),
	ids_codice_orig_FGN		VARCHAR(32),
	ids_codice_orig_DEP_SCA	VARCHAR(32)	
);

-- Lunghezze campi?
CREATE TABLE DBIAIT_ANALYSIS.ABITANTI_TRATTATI(
	idgis	VARCHAR(32),
	codice	VARCHAR(32),
	denom	VARCHAR(32),
	vol_civ	double precision,
	vol_ind	double precision,
	anno	INTEGER
	ae_civ	double precision,
	ae_ind	double precision,	
	ae_tot	double precision,
);


