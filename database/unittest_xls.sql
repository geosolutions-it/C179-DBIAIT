--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACCUMULI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ACCUMULI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_accumulo"."codice_ato" "codice_ato","acq_accumulo"."denom" "denom",FROM_FLOAT_TO_INT("acq_accumulo"."quota") "quota","acq_accumulo"."anno_costr" "anno_costr","acq_accumulo"."anno_ristr" "anno_ristr","acq_accumulo"."d_stato_cons" "d_stato_cons","acq_accumulo"."d_ubicazione" "d_ubicazione","acq_accumulo"."d_materiale" "d_materiale","acq_accumulo"."volume" "volume","acq_accumulo"."quota_fondo" "quota_fondo","acq_accumulo"."d_stato" "d_stato","acq_accumulo"."a_anno_costr" "a_anno_costr","acq_accumulo"."a_anno_ristr" "a_anno_ristr","acq_accumulo"."a_volume" "a_volume","acq_accumulo"."data_agg" "data_agg","acq_accumulo"."cod_comune" "cod_comune",GB_X("acq_accumulo"."geom") "transformed_x_geom",GB_Y("acq_accumulo"."geom") "transformed_y_geom","acq_auth_accum"."d_telecont" "d_telecont",TO_BIT("acq_auth_accum"."sn_strum_mis_liv") "sn_strum_mis_liv",TO_BIT("acq_auth_accum"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_accum"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_accum"."anno_instal_clor" "anno_instal_clor","acq_auth_accum"."anno_ristr_clor" "anno_ristr_clor","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","localita"."denominazi" "denominazi" FROM "acq_accumulo" "acq_accumulo" LEFT JOIN "acq_auth_accum" "acq_auth_accum" ON "acq_accumulo"."idgis"="acq_auth_accum"."id_accumulo" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_accumulo"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "acq_accumulo"."idgis"="schema_acq"."idgis" WHERE acq_accumulo.d_gestore = 'PUBLIACQUA' AND acq_accumulo.d_ambito IN ('AT3', NULL) AND acq_accumulo.d_stato NOT IN ('IPR','IAC') UNION ALL SELECT "a_acq_accumulo"."codice_ato" "codice_ato","a_acq_accumulo"."denom" "denom",FROM_FLOAT_TO_INT("a_acq_accumulo"."quota") "quota","a_acq_accumulo"."anno_costr" "anno_costr","a_acq_accumulo"."anno_ristr" "anno_ristr","a_acq_accumulo"."d_stato_cons" "d_stato_cons","a_acq_accumulo"."d_ubicazione" "d_ubicazione","a_acq_accumulo"."d_materiale" "d_materiale","a_acq_accumulo"."volume" "volume","a_acq_accumulo"."quota_fondo" "quota_fondo","a_acq_accumulo"."d_stato" "d_stato","a_acq_accumulo"."a_anno_costr" "a_anno_costr","a_acq_accumulo"."a_anno_ristr" "a_anno_ristr","a_acq_accumulo"."a_volume" "a_volume","a_acq_accumulo"."data_agg" "data_agg","a_acq_accumulo"."cod_comune" "cod_comune",GB_X("a_acq_accumulo"."geom") "transformed_x_geom",GB_Y("a_acq_accumulo"."geom") "transformed_y_geom","acq_auth_accum"."d_telecont" "d_telecont",TO_BIT("acq_auth_accum"."sn_strum_mis_liv") "sn_strum_mis_liv",TO_BIT("acq_auth_accum"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_accum"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_accum"."anno_instal_clor" "anno_instal_clor","acq_auth_accum"."anno_ristr_clor" "anno_ristr_clor","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","localita"."denominazi" "denominazi" FROM "a_acq_accumulo" "a_acq_accumulo" LEFT JOIN "acq_auth_accum" "acq_auth_accum" ON "a_acq_accumulo"."idgis"="acq_auth_accum"."id_accumulo" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_accumulo"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_accumulo"."idgis"="schema_acq"."idgis" WHERE a_acq_accumulo.d_gestore = 'PUBLIACQUA' AND a_acq_accumulo.d_ambito IN ('AT3', NULL) AND a_acq_accumulo.d_stato NOT IN ('IPR','IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ACCUMULI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACCUMULI_INADD_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ACCUMULI_INADD'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_adduzione" "ids_codice_adduzione","id_gestore_adduzione" "id_gestore_adduzione" FROM "accumuli_inadd" "accumuli_inadd"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ACCUMULI_INADD, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACCUMULI_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ACCUMULI_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "accumuli_inreti" "accumuli_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ACCUMULI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUT_COM_SERV_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ADDUT_COM_SERV'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","id_comune_istat" "id_comune_istat" FROM "addut_com_serv" "addut_com_serv"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ADDUT_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUT_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ADDUT_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "addut_inreti" "addut_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ADDUT_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUT_TRONCHI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ADDUT_TRONCHI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "addut_tronchi"."codice_ato" "codice_ato","addut_tronchi"."idgis" "idgis","addut_tronchi"."idgis_rete" "idgis_rete","addut_tronchi"."id_tipo_telecon" "id_tipo_telecon","addut_tronchi"."id_materiale" "id_materiale","addut_tronchi"."id_conservazione" "id_conservazione","addut_tronchi"."diametro" "diametro","addut_tronchi"."anno" "anno","addut_tronchi"."lunghezza" "lunghezza","addut_tronchi"."idx_materiale" "idx_materiale","addut_tronchi"."idx_diametro" "idx_diametro","addut_tronchi"."idx_anno" "idx_anno","addut_tronchi"."idx_lunghezza" "idx_lunghezza","addut_tronchi"."pressione" "pressione","addut_tronchi"."protezione_catodica" "protezione_catodica","addut_tronchi"."note" "note","addut_tronchi"."geom" "geom",TO_BIT("acq_condotta"."id_sist_prot_cat") "id_sist_prot_cat","acq_adduttrice"."codice_ato" "adduttrice_codice_ato","acq_condotta"."data_esercizio" "data_esercizio" FROM "addut_tronchi" "addut_tronchi" LEFT JOIN "acq_condotta" "acq_condotta" ON "addut_tronchi"."idgis"="acq_condotta"."idgis" LEFT JOIN "acq_adduttrice" "acq_adduttrice" ON "addut_tronchi"."idgis_rete"="acq_adduttrice"."idgis"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ADDUT_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUTTRICI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_ADDUTTRICI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_adduttrice"."codice_ato" "codice_ato","acq_adduttrice"."denom" "denom","acq_adduttrice"."portata_media" "portata_media","acq_adduttrice"."d_stato" "d_stato","acq_adduttrice"."a_portata_media" "a_portata_media","acq_adduttrice"."data_agg" "data_agg","acq_adduttrice"."nr_rip" "nr_rip",TO_BIT("acq_auth_adduttr"."sn_strum_mis_port") "sn_strum_mis_port","acq_lunghezza_rete"."lunghezza" "lunghezza","acq_lunghezza_rete"."lunghezza_tlc" "lunghezza_tlc","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","stats_cloratore"."counter" "count_cloratori","utenze_distribuzioni_adduttrici"."nr_utenze_dirette" "nr_utenze_dirette","utenze_distribuzioni_adduttrici"."nr_utenze_dir_dom_e_residente" "nr_utenze_dir_dom_e_residente","utenze_distribuzioni_adduttrici"."nr_utenze_dir_residente" "nr_utenze_dir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_condominiali" "nr_utenze_condominiali","utenze_distribuzioni_adduttrici"."nr_utenze_indir_indirette" "nr_utenze_indir_indirette","utenze_distribuzioni_adduttrici"."nr_utenze_indir_domestici" "nr_utenze_indir_domestici","utenze_distribuzioni_adduttrici"."nr_utenze_indir_residente" "nr_utenze_indir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_misuratore" "nr_utenze_misuratore","utenze_distribuzioni_adduttrici"."volume_erogato" "volume_erogato","utenze_distribuzioni_adduttrici"."volume_fatturato" "volume_fatturato" FROM "acq_adduttrice" "acq_adduttrice" LEFT JOIN "acq_auth_adduttr" "acq_auth_adduttr" ON "acq_adduttrice"."idgis"="acq_auth_adduttr"."id_adduttrice" LEFT JOIN "acq_lunghezza_rete" "acq_lunghezza_rete" ON "acq_adduttrice"."codice_ato"="acq_lunghezza_rete"."codice_ato" LEFT JOIN "stats_cloratore" "stats_cloratore" ON "acq_adduttrice"."idgis"="stats_cloratore"."id_rete" LEFT JOIN "schema_acq" "schema_acq" ON "acq_adduttrice"."idgis"="schema_acq"."idgis" LEFT JOIN "utenze_distribuzioni_adduttrici" "utenze_distribuzioni_adduttrici" ON "utenze_distribuzioni_adduttrici"."id_rete"="acq_adduttrice"."idgis" WHERE acq_adduttrice.d_gestore = 'PUBLIACQUA' AND acq_adduttrice.d_ambito IN ('AT3', NULL) AND acq_adduttrice.d_stato NOT IN ('IPR','IAC') UNION ALL SELECT "a_acq_adduttrice"."codice_ato" "codice_ato","a_acq_adduttrice"."denom" "denom","a_acq_adduttrice"."portata_media" "portata_media","a_acq_adduttrice"."d_stato" "d_stato","a_acq_adduttrice"."a_portata_media" "a_portata_media","a_acq_adduttrice"."data_agg" "data_agg","a_acq_adduttrice"."nr_rip" "nr_rip",TO_BIT("acq_auth_adduttr"."sn_strum_mis_port") "sn_strum_mis_port","acq_lunghezza_rete"."lunghezza" "lunghezza","acq_lunghezza_rete"."lunghezza_tlc" "lunghezza_tlc","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","stats_cloratore"."counter" "count_cloratori","utenze_distribuzioni_adduttrici"."nr_utenze_dirette" "nr_utenze_dirette","utenze_distribuzioni_adduttrici"."nr_utenze_dir_dom_e_residente" "nr_utenze_dir_dom_e_residente","utenze_distribuzioni_adduttrici"."nr_utenze_dir_residente" "nr_utenze_dir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_condominiali" "nr_utenze_condominiali","utenze_distribuzioni_adduttrici"."nr_utenze_indir_indirette" "nr_utenze_indir_indirette","utenze_distribuzioni_adduttrici"."nr_utenze_indir_domestici" "nr_utenze_indir_domestici","utenze_distribuzioni_adduttrici"."nr_utenze_indir_residente" "nr_utenze_indir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_misuratore" "nr_utenze_misuratore","utenze_distribuzioni_adduttrici"."volume_erogato" "volume_erogato","utenze_distribuzioni_adduttrici"."volume_fatturato" "volume_fatturato" FROM "a_acq_adduttrice" "a_acq_adduttrice" LEFT JOIN "acq_auth_adduttr" "acq_auth_adduttr" ON "a_acq_adduttrice"."idgis"="acq_auth_adduttr"."id_adduttrice" LEFT JOIN "acq_lunghezza_rete" "acq_lunghezza_rete" ON "a_acq_adduttrice"."codice_ato"="acq_lunghezza_rete"."codice_ato" LEFT JOIN "stats_cloratore" "stats_cloratore" ON "a_acq_adduttrice"."idgis"="stats_cloratore"."id_rete" LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_adduttrice"."idgis"="schema_acq"."idgis" LEFT JOIN "utenze_distribuzioni_adduttrici" "utenze_distribuzioni_adduttrici" ON "utenze_distribuzioni_adduttrici"."id_rete"="a_acq_adduttrice"."idgis" WHERE a_acq_adduttrice.d_gestore = 'PUBLIACQUA' AND a_acq_adduttrice.d_ambito IN ('AT3', NULL) AND a_acq_adduttrice.d_stato NOT IN ('IPR','IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_ADDUTTRICI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_COLLETT_COM_SERV_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_COLLETT_COM_SERV'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","id_comune_istat" "id_comune_istat" FROM "collet_com_serv" "collet_com_serv"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_COLLETT_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_COLLETT_TRONCHI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_COLLETT_TRONCHI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "collett_tronchi"."codice_ato" "codice_ato","collett_tronchi"."idgis" "idgis","collett_tronchi"."idgis_rete" "idgis_rete","collett_tronchi"."recapito" "recapito","collett_tronchi"."id_materiale" "id_materiale","collett_tronchi"."id_conservazione" "id_conservazione","collett_tronchi"."diametro" "diametro","collett_tronchi"."anno" "anno","collett_tronchi"."funziona_gravita" "funziona_gravita","collett_tronchi"."lunghezza" "lunghezza","collett_tronchi"."idx_materiale" "idx_materiale","collett_tronchi"."idx_diametro" "idx_diametro","collett_tronchi"."idx_anno" "idx_anno","collett_tronchi"."idx_lunghezza" "idx_lunghezza","collett_tronchi"."depurazione" "depurazione","collett_tronchi"."note" "note","collett_tronchi"."id_refluo_trasportato" "id_refluo_trasportato","collett_tronchi"."geom" "geom","fgn_condotta"."data_esercizio" "data_esercizio" FROM "collett_tronchi" "collett_tronchi" LEFT JOIN "fgn_condotta" "fgn_condotta" ON "collett_tronchi"."idgis"="fgn_condotta"."idgis"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_COLLETT_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_COLLETTORI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_COLLETTORI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "fgn_collettore"."codice_ato" "codice_ato","fgn_collettore"."denom" "denom","fgn_collettore"."d_stato" "d_stato","fgn_collettore"."data_agg" "data_agg","fgn_auth_collet"."a_vol_idra" "a_vol_idra","fgn_auth_collet"."vol_idraulici" "vol_idraulici",TO_BIT("fgn_auth_collet"."sn_scar_sup") "sn_scar_sup","fgn_auth_collet"."n_scar_piena" "n_scar_piena","fgn_auth_collet"."d_telecont" "d_telecont","fgn_auth_collet"."nr_rip" "nr_rip",TO_BIT("fgn_auth_collet"."sn_strum_mis_port") "sn_strum_mis_port","fgn_lunghezza_rete"."lunghezza" "lunghezza","utenze_fognature_collettori"."nr_utenze_totali" "utenze_totali","utenze_fognature_collettori"."nr_utenze_industriali" "nr_utenze_industriali","utenze_fognature_collettori"."volume_utenze_totali" "volume_utenze_totali","utenze_fognature_collettori"."volume_utenze_industriali" "volume_utenze_industriali" FROM "fgn_collettore" "fgn_collettore" LEFT JOIN "fgn_auth_collet" "fgn_auth_collet" ON "fgn_collettore"."idgis"="fgn_auth_collet"."id_collettore" LEFT JOIN "fgn_lunghezza_rete" "fgn_lunghezza_rete" ON "fgn_collettore"."codice_ato"="fgn_lunghezza_rete"."codice_ato" LEFT JOIN "utenze_fognature_collettori" "utenze_fognature_collettori" ON "fgn_collettore"."idgis"="utenze_fognature_collettori"."id_rete" WHERE fgn_collettore.d_gestore = 'PUBLIACQUA' AND fgn_collettore.d_ambito IN ('AT3', NULL) AND fgn_collettore.d_stato NOT IN ('IPR','IAC') UNION ALL SELECT "a_fgn_collettore"."codice_ato" "codice_ato","a_fgn_collettore"."denom" "denom","a_fgn_collettore"."d_stato" "d_stato","a_fgn_collettore"."data_agg" "data_agg","fgn_auth_collet"."a_vol_idra" "a_vol_idra","fgn_auth_collet"."vol_idraulici" "vol_idraulici",TO_BIT("fgn_auth_collet"."sn_scar_sup") "sn_scar_sup","fgn_auth_collet"."n_scar_piena" "n_scar_piena","fgn_auth_collet"."d_telecont" "d_telecont","fgn_auth_collet"."nr_rip" "nr_rip",TO_BIT("fgn_auth_collet"."sn_strum_mis_port") "sn_strum_mis_port","fgn_lunghezza_rete"."lunghezza" "lunghezza","utenze_fognature_collettori"."nr_utenze_totali" "utenze_totali","utenze_fognature_collettori"."nr_utenze_industriali" "nr_utenze_industriali","utenze_fognature_collettori"."volume_utenze_totali" "volume_utenze_totali","utenze_fognature_collettori"."volume_utenze_industriali" "volume_utenze_industriali" FROM "a_fgn_collettore" "a_fgn_collettore" LEFT JOIN "fgn_auth_collet" "fgn_auth_collet" ON "a_fgn_collettore"."idgis"="fgn_auth_collet"."id_collettore" LEFT JOIN "fgn_lunghezza_rete" "fgn_lunghezza_rete" ON "a_fgn_collettore"."codice_ato"="fgn_lunghezza_rete"."codice_ato" LEFT JOIN "utenze_fognature_collettori" "utenze_fognature_collettori" ON "a_fgn_collettore"."idgis"="utenze_fognature_collettori"."id_rete" WHERE a_fgn_collettore.d_gestore = 'PUBLIACQUA' AND a_fgn_collettore.d_ambito IN ('AT3', NULL) AND a_fgn_collettore.d_stato NOT IN ('IPR','IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_COLLETTORI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DEPURAT_INCOLL_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DEPURAT_INCOLL'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_collettore" "ids_codice_collettore","id_gestore_collettore" "id_gestore_collettore" FROM "depurato_incoll" "depurato_incoll"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DEPURAT_INCOLL, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DEPURAT_POMPE_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DEPURAT_POMPE'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_ato" "codice_ato","d_stato_cons" "d_stato_cons","anno_instal" "anno_instal","anno_ristr" "anno_ristr","potenza" "potenza","portata" "portata","prevalenza" "prevalenza","sn_riserva" "sn_riserva","idx_anno_instal" "idx_anno_instal","idx_anno_ristr" "idx_anno_ristr","idx_potenza" "idx_potenza","idx_portata" "idx_portata","idx_prevalenza" "idx_prevalenza" FROM "depurato_pompe" "depurato_pompe"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DEPURAT_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DEPURATORI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DEPURATORI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "fgn_trattamento"."codice_ato" "codice_ato","fgn_trattamento"."denom" "denom",FROM_FLOAT_TO_INT("fgn_trattamento"."quota") "quota","fgn_trattamento"."d_corpo_ricet" "d_corpo_ricet","fgn_trattamento"."pot_prog" "pot_prog","fgn_trattamento"."conc_co2_ing" "conc_co2_ing","fgn_trattamento"."conc_co2_usc" "conc_co2_usc","fgn_trattamento"."vol_att_tratt" "vol_att_tratt","fgn_trattamento"."anno_costr" "anno_costr",TO_BIT("fgn_auth_trattam"."sn_terziario") "sn_terziario",TO_BIT("fgn_auth_trattam"."sn_imhoff") "sn_imhoff","fgn_trattamento"."d_stato" "d_stato","fgn_trattamento"."a_anno_costr" "a_anno_costr","fgn_trattamento"."data_agg" "data_agg","fgn_auth_trattam"."anno_ristr_civ" "anno_ristr_civ","fgn_trattamento"."cod_comune" "cod_comune",GB_X("fgn_trattamento"."geom") "transformed_x_geom",GB_Y("fgn_trattamento"."geom") "transformed_y_geom","fgn_auth_trattam"."d_stato_cons_civ" "d_stato_cons_civ","fgn_auth_trattam"."anno_ristr_elmec" "anno_ristr_elmec","fgn_auth_trattam"."d_stato_cons_elmec" "d_stato_cons_elmec","fgn_auth_trattam"."vol_riutilizzo" "vol_riutilizzo","fgn_auth_trattam"."d_telecont" "d_telecont",TO_BIT("fgn_auth_trattam"."sn_319_76") "sn_319_76","fgn_auth_trattam"."potenza_instal" "potenza_instal","fgn_auth_trattam"."ene_auto_prod" "ene_auto_prod",TO_BIT("fgn_auth_trattam"."sn_presid") "sn_presid","fgn_auth_trattam"."d_tipo_tratt_acqua" "d_tipo_tratt_acqua","fgn_auth_trattam"."n_linee_acq" "n_linee_acq",TO_BIT("fgn_auth_trattam"."sn_biodischi") "sn_biodischi",TO_BIT("fgn_auth_trattam"."sn_letti_percol") "sn_letti_percol",TO_BIT("fgn_auth_trattam"."sn_equaliz") "sn_equaliz",TO_BIT("fgn_auth_trattam"."sn_grig_trad") "sn_grig_trad",TO_BIT("fgn_auth_trattam"."sn_grig_spinta") "sn_grig_spinta",TO_BIT("fgn_auth_trattam"."sn_dissab") "sn_dissab",TO_BIT("fgn_auth_trattam"."sn_disoleat") "sn_disoleat",TO_BIT("fgn_auth_trattam"."sn_sedim_prim") "sn_sedim_prim",TO_BIT("fgn_auth_trattam"."sn_denitr") "sn_denitr",TO_BIT("fgn_auth_trattam"."sn_oss_c_nitri") "sn_oss_c_nitri",TO_BIT("fgn_auth_trattam"."sn_oss_s_nitri") "sn_oss_s_nitri",TO_BIT("fgn_auth_trattam"."sn_defosf_sim") "sn_defosf_sim",TO_BIT("fgn_auth_trattam"."sn_sedim_sec") "sn_sedim_sec",TO_BIT("fgn_auth_trattam"."sn_chirif_defos") "sn_chirif_defos",TO_BIT("fgn_auth_trattam"."sn_filtr_sab") "sn_filtr_sab","fgn_auth_trattam"."d_ass_carb_att" "d_ass_carb_att",TO_BIT("fgn_auth_trattam"."sn_deodor") "sn_deodor",TO_BIT("fgn_auth_trattam"."sn_disinf") "sn_disinf","fgn_auth_trattam"."d_tipo_tratt_fanghi" "d_tipo_tratt_fanghi","fgn_auth_trattam"."n_linee_fanghi" "n_linee_fanghi",TO_BIT("fgn_auth_trattam"."sn_ispess") "sn_ispess","fgn_auth_trattam"."d_digest_anae" "d_digest_anae",TO_BIT("fgn_auth_trattam"."sn_digest_aerob") "sn_digest_aerob",TO_BIT("fgn_auth_trattam"."sn_post_ispes") "sn_post_ispes",TO_BIT("fgn_auth_trattam"."sn_essic_letto") "sn_essic_letto","fgn_auth_trattam"."d_disidrat" "d_disidrat",TO_BIT("fgn_auth_trattam"."sn_essic_term") "sn_essic_term",TO_BIT("fgn_auth_trattam"."sn_incen_term") "sn_incen_term","fgn_auth_trattam"."d_dest_fanghi" "d_dest_fanghi","abitanti_trattati"."vol_civ" "liquami_civili","abitanti_trattati"."vol_ind" "liquami_industr","fgn_auth_trattam"."percolati" "percolati","fgn_auth_trattam"."bottini" "bottini","fgn_auth_trattam"."umid_media_res" "umid_media_res","fgn_auth_trattam"."peso_fanghi_tlq" "peso_fanghi_tlq","fgn_auth_trattam"."peso_fanghi_tlq_agr" "peso_fanghi_tlq_agr","fgn_auth_trattam"."peso_fanghi_tlq_riut_compost" "peso_fanghi_tlq_riut_compost","fgn_auth_trattam"."peso_fanghi_tlq_riut_combust" "peso_fanghi_tlq_riut_combust","fgn_auth_trattam"."peso_fanghi_tlq_riut_altro" "peso_fanghi_tlq_riut_altro","fgn_auth_trattam"."peso_fanghi_tlq_smalt" "peso_fanghi_tlq_smalt","fgn_auth_trattam"."peso_fanghi_tlq_disc" "peso_fanghi_tlq_disc","fgn_auth_trattam"."trat_fanghi_sma" "trat_fanghi_sma","fgn_auth_trattam"."trat_fanghi_sma_disc" "trat_fanghi_sma_disc","fgn_auth_trattam"."trat_acq_altro" "trat_acq_altro","fgn_auth_trattam"."a_anno_ristr_civ" "a_anno_ristr_civ","fgn_auth_trattam"."a_anno_ristr_elmec" "a_anno_ristr_elmec","fgn_auth_trattam"."a_pot_prog" "a_pot_prog","fgn_auth_trattam"."a_carico_att_tot" "a_carico_att_tot","fgn_auth_trattam"."a_carico_civ_att" "a_carico_civ_att","fgn_auth_trattam"."a_conc_co2_ing" "a_conc_co2_ing","fgn_auth_trattam"."a_conc_co2_usc" "a_conc_co2_usc","fgn_auth_trattam"."a_vol_att_tratt" "a_vol_att_tratt","fgn_auth_trattam"."consumo_ee" "consumo_ee","fgn_auth_trattam"."a_potenza_instal" "a_potenza_instal",TO_BIT("fgn_auth_trattam"."sn_fitodep") "sn_fitodep",TO_BIT("fgn_auth_trattam"."sn_lagunaggio") "sn_lagunaggio","localita"."denominazi" "denominazi",TO_BIT("fgn_auth_trattam"."sn_strum_mis_port_in") "sn_strum_mis_port_in",TO_BIT("fgn_auth_trattam"."sn_strum_mis_port_out") "sn_strum_mis_port_out","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","abitanti_trattati"."ae_tot" "ae_tot","abitanti_trattati"."ae_civ" "ae_civ" FROM "fgn_trattamento" "fgn_trattamento" LEFT JOIN "fgn_auth_trattam" "fgn_auth_trattam" ON "fgn_trattamento"."idgis"="fgn_auth_trattam"."id_trattamento" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","fgn_trattamento"."geom") LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="fgn_trattamento"."codice_ato" LEFT JOIN "abitanti_trattati" "abitanti_trattati" ON "fgn_trattamento"."idgis"="abitanti_trattati"."idgis" WHERE fgn_trattamento.d_gestore = 'PUBLIACQUA' AND fgn_trattamento.d_ambito IN ('AT3', NULL) AND fgn_trattamento.d_stato NOT IN ('IPR','IAC') UNION ALL SELECT "a_fgn_trattamento"."codice_ato" "codice_ato","a_fgn_trattamento"."denom" "denom",FROM_FLOAT_TO_INT("a_fgn_trattamento"."quota") "quota","a_fgn_trattamento"."d_corpo_ricet" "d_corpo_ricet","a_fgn_trattamento"."pot_prog" "pot_prog","a_fgn_trattamento"."conc_co2_ing" "conc_co2_ing","a_fgn_trattamento"."conc_co2_usc" "conc_co2_usc","a_fgn_trattamento"."vol_att_tratt" "vol_att_tratt","a_fgn_trattamento"."anno_costr" "anno_costr",TO_BIT("fgn_auth_trattam"."sn_terziario") "sn_terziario",TO_BIT("fgn_auth_trattam"."sn_imhoff") "sn_imhoff","a_fgn_trattamento"."d_stato" "d_stato","a_fgn_trattamento"."a_anno_costr" "a_anno_costr","a_fgn_trattamento"."data_agg" "data_agg","fgn_auth_trattam"."anno_ristr_civ" "anno_ristr_civ","a_fgn_trattamento"."cod_comune" "cod_comune",GB_X("a_fgn_trattamento"."geom") "transformed_x_geom",GB_Y("a_fgn_trattamento"."geom") "transformed_y_geom","fgn_auth_trattam"."d_stato_cons_civ" "d_stato_cons_civ","fgn_auth_trattam"."anno_ristr_elmec" "anno_ristr_elmec","fgn_auth_trattam"."d_stato_cons_elmec" "d_stato_cons_elmec","fgn_auth_trattam"."vol_riutilizzo" "vol_riutilizzo","fgn_auth_trattam"."d_telecont" "d_telecont",TO_BIT("fgn_auth_trattam"."sn_319_76") "sn_319_76","fgn_auth_trattam"."potenza_instal" "potenza_instal","fgn_auth_trattam"."ene_auto_prod" "ene_auto_prod",TO_BIT("fgn_auth_trattam"."sn_presid") "sn_presid","fgn_auth_trattam"."d_tipo_tratt_acqua" "d_tipo_tratt_acqua","fgn_auth_trattam"."n_linee_acq" "n_linee_acq",TO_BIT("fgn_auth_trattam"."sn_biodischi") "sn_biodischi",TO_BIT("fgn_auth_trattam"."sn_letti_percol") "sn_letti_percol",TO_BIT("fgn_auth_trattam"."sn_equaliz") "sn_equaliz",TO_BIT("fgn_auth_trattam"."sn_grig_trad") "sn_grig_trad",TO_BIT("fgn_auth_trattam"."sn_grig_spinta") "sn_grig_spinta",TO_BIT("fgn_auth_trattam"."sn_dissab") "sn_dissab",TO_BIT("fgn_auth_trattam"."sn_disoleat") "sn_disoleat",TO_BIT("fgn_auth_trattam"."sn_sedim_prim") "sn_sedim_prim",TO_BIT("fgn_auth_trattam"."sn_denitr") "sn_denitr",TO_BIT("fgn_auth_trattam"."sn_oss_c_nitri") "sn_oss_c_nitri",TO_BIT("fgn_auth_trattam"."sn_oss_s_nitri") "sn_oss_s_nitri",TO_BIT("fgn_auth_trattam"."sn_defosf_sim") "sn_defosf_sim",TO_BIT("fgn_auth_trattam"."sn_sedim_sec") "sn_sedim_sec",TO_BIT("fgn_auth_trattam"."sn_chirif_defos") "sn_chirif_defos",TO_BIT("fgn_auth_trattam"."sn_filtr_sab") "sn_filtr_sab","fgn_auth_trattam"."d_ass_carb_att" "d_ass_carb_att",TO_BIT("fgn_auth_trattam"."sn_deodor") "sn_deodor",TO_BIT("fgn_auth_trattam"."sn_disinf") "sn_disinf","fgn_auth_trattam"."d_tipo_tratt_fanghi" "d_tipo_tratt_fanghi","fgn_auth_trattam"."n_linee_fanghi" "n_linee_fanghi",TO_BIT("fgn_auth_trattam"."sn_ispess") "sn_ispess","fgn_auth_trattam"."d_digest_anae" "d_digest_anae",TO_BIT("fgn_auth_trattam"."sn_digest_aerob") "sn_digest_aerob",TO_BIT("fgn_auth_trattam"."sn_post_ispes") "sn_post_ispes",TO_BIT("fgn_auth_trattam"."sn_essic_letto") "sn_essic_letto","fgn_auth_trattam"."d_disidrat" "d_disidrat",TO_BIT("fgn_auth_trattam"."sn_essic_term") "sn_essic_term",TO_BIT("fgn_auth_trattam"."sn_incen_term") "sn_incen_term","fgn_auth_trattam"."d_dest_fanghi" "d_dest_fanghi","abitanti_trattati"."vol_civ" "liquami_civili","abitanti_trattati"."vol_ind" "liquami_industr","fgn_auth_trattam"."percolati" "percolati","fgn_auth_trattam"."bottini" "bottini","fgn_auth_trattam"."umid_media_res" "umid_media_res","fgn_auth_trattam"."peso_fanghi_tlq" "peso_fanghi_tlq","fgn_auth_trattam"."peso_fanghi_tlq_agr" "peso_fanghi_tlq_agr","fgn_auth_trattam"."peso_fanghi_tlq_riut_compost" "peso_fanghi_tlq_riut_compost","fgn_auth_trattam"."peso_fanghi_tlq_riut_combust" "peso_fanghi_tlq_riut_combust","fgn_auth_trattam"."peso_fanghi_tlq_riut_altro" "peso_fanghi_tlq_riut_altro","fgn_auth_trattam"."peso_fanghi_tlq_smalt" "peso_fanghi_tlq_smalt","fgn_auth_trattam"."peso_fanghi_tlq_disc" "peso_fanghi_tlq_disc","fgn_auth_trattam"."trat_fanghi_sma" "trat_fanghi_sma","fgn_auth_trattam"."trat_fanghi_sma_disc" "trat_fanghi_sma_disc","fgn_auth_trattam"."trat_acq_altro" "trat_acq_altro","fgn_auth_trattam"."a_anno_ristr_civ" "a_anno_ristr_civ","fgn_auth_trattam"."a_anno_ristr_elmec" "a_anno_ristr_elmec","fgn_auth_trattam"."a_pot_prog" "a_pot_prog","fgn_auth_trattam"."a_carico_att_tot" "a_carico_att_tot","fgn_auth_trattam"."a_carico_civ_att" "a_carico_civ_att","fgn_auth_trattam"."a_conc_co2_ing" "a_conc_co2_ing","fgn_auth_trattam"."a_conc_co2_usc" "a_conc_co2_usc","fgn_auth_trattam"."a_vol_att_tratt" "a_vol_att_tratt","fgn_auth_trattam"."consumo_ee" "consumo_ee","fgn_auth_trattam"."a_potenza_instal" "a_potenza_instal",TO_BIT("fgn_auth_trattam"."sn_fitodep") "sn_fitodep",TO_BIT("fgn_auth_trattam"."sn_lagunaggio") "sn_lagunaggio","localita"."denominazi" "denominazi",TO_BIT("fgn_auth_trattam"."sn_strum_mis_port_in") "sn_strum_mis_port_in",TO_BIT("fgn_auth_trattam"."sn_strum_mis_port_out") "sn_strum_mis_port_out","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","abitanti_trattati"."ae_tot" "ae_tot","abitanti_trattati"."ae_civ" "ae_civ" FROM "a_fgn_trattamento" "a_fgn_trattamento" LEFT JOIN "fgn_auth_trattam" "fgn_auth_trattam" ON "a_fgn_trattamento"."idgis"="fgn_auth_trattam"."id_trattamento" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_fgn_trattamento"."geom") LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="a_fgn_trattamento"."codice_ato" LEFT JOIN "abitanti_trattati" "abitanti_trattati" ON "a_fgn_trattamento"."idgis"="abitanti_trattati"."idgis" WHERE a_fgn_trattamento.d_gestore = 'PUBLIACQUA' AND a_fgn_trattamento.d_ambito IN ('AT3', NULL) AND a_fgn_trattamento.d_stato NOT IN ('IPR','IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DEPURATORI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIB_COM_SERV_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DISTRIB_COM_SERV'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_opera" "codice_opera","id_comune_istat" "id_comune_istat","perc_popsrv" "perc_popsrv" FROM "distrib_com_serv" "distrib_com_serv"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DISTRIB_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIB_LOC_SERV_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DISTRIB_LOC_SERV'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_opera" "codice_opera","id_localita_istat" "id_localita_istat","perc_popsrv" "perc_popsrv" FROM "distrib_loc_serv" "distrib_loc_serv"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DISTRIB_LOC_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIB_TRONCHI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DISTRIB_TRONCHI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "distrib_tronchi"."codice_ato" "codice_ato","distrib_tronchi"."idgis" "idgis","distrib_tronchi"."idgis_rete" "idgis_rete","distrib_tronchi"."id_tipo_telecon" "id_tipo_telecon","distrib_tronchi"."id_materiale" "id_materiale","distrib_tronchi"."id_conservazione" "id_conservazione","distrib_tronchi"."diametro" "diametro","distrib_tronchi"."anno" "anno","distrib_tronchi"."lunghezza" "lunghezza","distrib_tronchi"."idx_materiale" "idx_materiale","distrib_tronchi"."idx_diametro" "idx_diametro","distrib_tronchi"."idx_anno" "idx_anno","distrib_tronchi"."idx_lunghezza" "idx_lunghezza","distrib_tronchi"."pressione" "pressione","distrib_tronchi"."note" "note","distrib_tronchi"."geom" "geom",TO_YEAR("acq_condotta"."data_esercizio") "data_esercizio" FROM "distrib_tronchi" "distrib_tronchi" LEFT JOIN "acq_rete_distrib" "acq_rete_distrib" ON "distrib_tronchi"."idgis_rete"="acq_rete_distrib"."idgis" LEFT JOIN "acq_condotta" "acq_condotta" ON "distrib_tronchi"."idgis"="acq_condotta"."idgis"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DISTRIB_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIBUZIONI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_DISTRIBUZIONI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_rete_distrib"."codice_ato" "codice_ato","acq_rete_distrib"."denom" "denom","acq_rete_distrib"."vol_immesso" "vol_immesso","acq_rete_distrib"."vol_imm_terzi" "vol_imm_terzi","acq_rete_distrib"."vol_ceduto" "vol_ceduto","acq_rete_distrib"."d_stato" "d_stato","acq_rete_distrib"."a_vol_immesso" "a_vol_immesso","acq_rete_distrib"."a_vol_imm_terzi" "a_vol_imm_terzi","acq_rete_distrib"."a_vol_ceduto" "a_vol_ceduto","acq_rete_distrib"."data_agg" "data_agg",TO_BIT("acq_auth_rete_dist"."sn_ili") "sn_ili","acq_auth_rete_dist"."a_ili" "a_ili","acq_auth_rete_dist"."pres_es_max" "pres_es_max","acq_auth_rete_dist"."a_pres_es_max" "a_pres_es_max","acq_auth_rete_dist"."pres_es_min" "pres_es_min","acq_auth_rete_dist"."a_pres_es_min" "a_pres_es_min","acq_lunghezza_rete"."lunghezza_tlc" "lunghezza_tlc","utenze_distribuzioni_adduttrici"."nr_utenze_dirette" "nr_utenze_dirette","utenze_distribuzioni_adduttrici"."nr_utenze_dir_dom_e_residente" "nr_utenze_dir_dom_e_residente","utenze_distribuzioni_adduttrici"."nr_utenze_dir_residente" "nr_utenze_dir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_condominiali" "nr_utenze_condominiali","utenze_distribuzioni_adduttrici"."nr_utenze_indir_indirette" "nr_utenze_indir_indirette","utenze_distribuzioni_adduttrici"."nr_utenze_indir_domestici" "nr_utenze_indir_domestici","utenze_distribuzioni_adduttrici"."nr_utenze_indir_residente" "nr_utenze_indir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_misuratore" "nr_utenze_misuratore","utenze_distribuzioni_adduttrici"."volume_erogato" "volume_erogato","utenze_distribuzioni_adduttrici"."volume_fatturato" "volume_fatturato","utenze_distribuzioni_adduttrici"."nr_allacci" "nr_allacci","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","stats_cloratore"."counter" "count_cloratori","acq_lunghezza_rete"."lunghezza" "lunghezza" FROM "acq_rete_distrib" "acq_rete_distrib" LEFT JOIN "acq_auth_rete_dist" "acq_auth_rete_dist" ON "acq_rete_distrib"."idgis"="acq_auth_rete_dist"."id_rete_distrib" LEFT JOIN "acq_lunghezza_rete" "acq_lunghezza_rete" ON "acq_lunghezza_rete"."idgis"="acq_rete_distrib"."idgis" LEFT JOIN "acq_vol_utenze" "acq_vol_utenze" ON "acq_vol_utenze"."ids_codice_orig_acq"="acq_rete_distrib"."codice_ato" LEFT JOIN "utenze_distribuzioni_adduttrici" "utenze_distribuzioni_adduttrici" ON "utenze_distribuzioni_adduttrici"."id_rete"="acq_rete_distrib"."idgis" LEFT JOIN "stats_cloratore" "stats_cloratore" ON "acq_rete_distrib"."idgis"="stats_cloratore"."id_rete" LEFT JOIN "schema_acq" "schema_acq" ON "acq_rete_distrib"."idgis"="schema_acq"."idgis" WHERE acq_rete_distrib.d_gestore = 'PUBLIACQUA' AND acq_rete_distrib.d_ambito IN ('AT3', NULL) AND acq_rete_distrib.d_stato NOT IN ('IPR','IAC') UNION ALL SELECT "a_acq_rete_distrib"."codice_ato" "codice_ato","a_acq_rete_distrib"."denom" "denom","a_acq_rete_distrib"."vol_immesso" "vol_immesso","a_acq_rete_distrib"."vol_imm_terzi" "vol_imm_terzi","a_acq_rete_distrib"."vol_ceduto" "vol_ceduto","a_acq_rete_distrib"."d_stato" "d_stato","a_acq_rete_distrib"."a_vol_immesso" "a_vol_immesso","a_acq_rete_distrib"."a_vol_imm_terzi" "a_vol_imm_terzi","a_acq_rete_distrib"."a_vol_ceduto" "a_vol_ceduto","a_acq_rete_distrib"."data_agg" "data_agg",TO_BIT("acq_auth_rete_dist"."sn_ili") "sn_ili","acq_auth_rete_dist"."a_ili" "a_ili","acq_auth_rete_dist"."pres_es_max" "pres_es_max","acq_auth_rete_dist"."a_pres_es_max" "a_pres_es_max","acq_auth_rete_dist"."pres_es_min" "pres_es_min","acq_auth_rete_dist"."a_pres_es_min" "a_pres_es_min","acq_lunghezza_rete"."lunghezza_tlc" "lunghezza_tlc","utenze_distribuzioni_adduttrici"."nr_utenze_dirette" "nr_utenze_dirette","utenze_distribuzioni_adduttrici"."nr_utenze_dir_dom_e_residente" "nr_utenze_dir_dom_e_residente","utenze_distribuzioni_adduttrici"."nr_utenze_dir_residente" "nr_utenze_dir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_condominiali" "nr_utenze_condominiali","utenze_distribuzioni_adduttrici"."nr_utenze_indir_indirette" "nr_utenze_indir_indirette","utenze_distribuzioni_adduttrici"."nr_utenze_indir_domestici" "nr_utenze_indir_domestici","utenze_distribuzioni_adduttrici"."nr_utenze_indir_residente" "nr_utenze_indir_residente","utenze_distribuzioni_adduttrici"."nr_utenze_misuratore" "nr_utenze_misuratore","utenze_distribuzioni_adduttrici"."volume_erogato" "volume_erogato","utenze_distribuzioni_adduttrici"."volume_fatturato" "volume_fatturato","utenze_distribuzioni_adduttrici"."nr_allacci" "nr_allacci","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","stats_cloratore"."counter" "count_cloratori","acq_lunghezza_rete"."lunghezza" "lunghezza" FROM "a_acq_rete_distrib" "a_acq_rete_distrib" LEFT JOIN "acq_auth_rete_dist" "acq_auth_rete_dist" ON "a_acq_rete_distrib"."idgis"="acq_auth_rete_dist"."id_rete_distrib" LEFT JOIN "acq_lunghezza_rete" "acq_lunghezza_rete" ON "acq_lunghezza_rete"."idgis"="a_acq_rete_distrib"."idgis" LEFT JOIN "acq_vol_utenze" "acq_vol_utenze" ON "acq_vol_utenze"."ids_codice_orig_acq"="a_acq_rete_distrib"."codice_ato" LEFT JOIN "utenze_distribuzioni_adduttrici" "utenze_distribuzioni_adduttrici" ON "utenze_distribuzioni_adduttrici"."id_rete"="a_acq_rete_distrib"."idgis" LEFT JOIN "stats_cloratore" "stats_cloratore" ON "a_acq_rete_distrib"."idgis"="stats_cloratore"."id_rete" LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_rete_distrib"."idgis"="schema_acq"."idgis" WHERE a_acq_rete_distrib.d_gestore = 'PUBLIACQUA' AND a_acq_rete_distrib.d_ambito IN ('ATO3', NULL) AND a_acq_rete_distrib.d_stato NOT IN ('IPR','IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_DISTRIBUZIONI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FIUMI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FIUMI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_captazione"."codice_ato" "codice_ato","acq_captazione"."denom" "denom","acq_captazione"."corso_acqua" "corso_acqua",FROM_FLOAT_TO_INT("acq_captazione"."quota") "quota","acq_captazione"."anno_costr" "anno_costr","acq_captazione"."anno_ristr" "anno_ristr","acq_captazione"."d_stato_cons" "d_stato_cons","acq_captazione"."volume_medio_prel" "volume_medio_prel","acq_captazione"."d_stato" "d_stato","acq_captazione"."a_anno_costr" "a_anno_costr","acq_captazione"."a_anno_ristr" "a_anno_ristr","acq_captazione"."a_volume" "a_volume","acq_captazione"."data_agg" "data_agg","acq_auth_capt"."bacino" "bacino","acq_auth_capt"."d_classific" "d_classific","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo","acq_auth_capt"."area_bac_affer" "area_bac_affer",TO_BIT("acq_auth_capt"."sn_trav_fluv") "sn_trav_fluv",TO_BIT("acq_auth_capt"."sn_cam_presa") "sn_cam_presa",TO_BIT("acq_auth_capt"."sn_presa_succ") "sn_presa_succ",TO_BIT("acq_auth_capt"."sn_grigliat") "sn_grigliat",TO_BIT("acq_auth_capt"."sn_filtro") "sn_filtro",TO_BIT("acq_auth_capt"."sn_dissab") "sn_dissab","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor","acq_auth_capt"."a_area_bac_affer" "a_area_bac_affer","acq_captazione"."cod_comune" "cod_comune",GB_X("acq_captazione"."geom") "transformed_x_geom",GB_Y("acq_captazione"."geom") "transformed_y_geom","acq_capt_conces"."port_uti_max" "port_uti_max","acq_capt_conces"."port_uti_min" "port_uti_min","acq_capt_conces"."port_deriv" "port_deriv","acq_capt_conces"."estremi_conces" "estremi_conces","acq_capt_conces"."port_potab" "port_potab","acq_capt_conces"."a_port_es" "a_port_es","acq_capt_conces"."a_port_uti_max" "a_port_uti_max","acq_capt_conces"."a_port_uti_min" "a_port_uti_min","acq_capt_conces"."a_port_deriv" "a_port_deriv",IS_NULL("fiumi_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","localita"."denominazi" "denominazi",TO_BIT("acq_auth_capt"."sn_tut_ass") "sn_tut_ass",TO_BIT("acq_auth_capt"."sn_rispetto") "sn_rispetto",TO_BIT("acq_auth_capt"."sn_protezione") "sn_protezione","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "acq_captazione" "acq_captazione" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "acq_captazione"."idgis"="acq_auth_capt"."id_captazione" LEFT JOIN "acq_capt_conces" "acq_capt_conces" ON "acq_captazione"."idgis"="acq_capt_conces"."id_captazione" LEFT JOIN "fiumi_inpotab" "fiumi_inpotab" ON "fiumi_inpotab"."ids_codice"="acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_captazione"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "acq_captazione"."idgis"="schema_acq"."idgis" WHERE acq_captazione.d_gestore = 'PUBLIACQUA' AND acq_captazione.d_ambito IN ('AT3', NULL) AND acq_captazione.d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=0 UNION ALL SELECT "a_acq_captazione"."codice_ato" "codice_ato","a_acq_captazione"."denom" "denom","a_acq_captazione"."corso_acqua" "corso_acqua",FROM_FLOAT_TO_INT("a_acq_captazione"."quota") "quota","a_acq_captazione"."anno_costr" "anno_costr","a_acq_captazione"."anno_ristr" "anno_ristr","a_acq_captazione"."d_stato_cons" "d_stato_cons","a_acq_captazione"."volume_medio_prel" "volume_medio_prel","a_acq_captazione"."d_stato" "d_stato","a_acq_captazione"."a_anno_costr" "a_anno_costr","a_acq_captazione"."a_anno_ristr" "a_anno_ristr","a_acq_captazione"."a_volume" "a_volume","a_acq_captazione"."data_agg" "data_agg","acq_auth_capt"."bacino" "bacino","acq_auth_capt"."d_classific" "d_classific","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo","acq_auth_capt"."area_bac_affer" "area_bac_affer",TO_BIT("acq_auth_capt"."sn_trav_fluv") "sn_trav_fluv",TO_BIT("acq_auth_capt"."sn_cam_presa") "sn_cam_presa",TO_BIT("acq_auth_capt"."sn_presa_succ") "sn_presa_succ",TO_BIT("acq_auth_capt"."sn_grigliat") "sn_grigliat",TO_BIT("acq_auth_capt"."sn_filtro") "sn_filtro",TO_BIT("acq_auth_capt"."sn_dissab") "sn_dissab","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor","acq_auth_capt"."a_area_bac_affer" "a_area_bac_affer","a_acq_captazione"."cod_comune" "cod_comune",GB_X("a_acq_captazione"."geom") "transformed_x_geom",GB_Y("a_acq_captazione"."geom") "transformed_y_geom","a_acq_capt_conces"."port_uti_max" "port_uti_max","a_acq_capt_conces"."port_uti_min" "port_uti_min","a_acq_capt_conces"."port_deriv" "port_deriv","a_acq_capt_conces"."estremi_conces" "estremi_conces","a_acq_capt_conces"."port_potab" "port_potab","a_acq_capt_conces"."a_port_es" "a_port_es","a_acq_capt_conces"."a_port_uti_max" "a_port_uti_max","a_acq_capt_conces"."a_port_uti_min" "a_port_uti_min","a_acq_capt_conces"."a_port_deriv" "a_port_deriv",IS_NULL("fiumi_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","localita"."denominazi" "denominazi",TO_BIT("acq_auth_capt"."sn_tut_ass") "sn_tut_ass",TO_BIT("acq_auth_capt"."sn_rispetto") "sn_rispetto",TO_BIT("acq_auth_capt"."sn_protezione") "sn_protezione","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "a_acq_captazione" "a_acq_captazione" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "a_acq_captazione"."idgis"="acq_auth_capt"."id_captazione" LEFT JOIN "a_acq_capt_conces" "a_acq_capt_conces" ON "a_acq_captazione"."idgis"="a_acq_capt_conces"."id_captazione" LEFT JOIN "fiumi_inpotab" "fiumi_inpotab" ON "fiumi_inpotab"."ids_codice"="a_acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_captazione"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_captazione"."idgis"="schema_acq"."idgis" WHERE a_acq_captazione.d_gestore = 'PUBLIACQUA' AND a_acq_captazione.d_ambito IN ('AT3', NULL) AND a_acq_captazione.d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=0
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FIUMI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FIUMI_INPOTAB_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FIUMI_INPOTAB'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_potab" "ids_codice_potab","id_gestore_potab" "id_gestore_potab" FROM "fiumi_inpotab" "fiumi_inpotab"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FIUMI_INPOTAB, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FIUMI_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FIUMI_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "fiumi_inreti" "fiumi_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FIUMI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FOGNAT_TRONCHI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FOGNAT_TRONCHI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "fognat_tronchi"."idgis" "idgis","fognat_tronchi"."codice_ato" "codice_ato","fognat_tronchi"."idgis_rete" "idgis_rete","fognat_tronchi"."recapito" "recapito","fognat_tronchi"."id_materiale" "id_materiale","fognat_tronchi"."id_conservazione" "id_conservazione","fognat_tronchi"."diametro" "diametro","fognat_tronchi"."anno" "anno","fognat_tronchi"."funziona_gravita" "funziona_gravita","fognat_tronchi"."lunghezza" "lunghezza","fognat_tronchi"."idx_materiale" "idx_materiale","fognat_tronchi"."idx_diametro" "idx_diametro","fognat_tronchi"."idx_anno" "idx_anno","fognat_tronchi"."idx_lunghezza" "idx_lunghezza","fognat_tronchi"."depurazione" "depurazione","fognat_tronchi"."id_refluo_trasportato" "id_refluo_trasportato","fgn_condotta"."data_esercizio" "data_esercizio" FROM "fognat_tronchi" "fognat_tronchi" LEFT JOIN "fgn_condotta" "fgn_condotta" ON "fgn_condotta"."idgis"="fognat_tronchi"."idgis"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FOGNAT_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FOGNAT_COM_SERV_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FOGNAT_COM_SERV'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_opera" "codice_opera","id_comune_istat" "id_comune_istat","perc_popsrv" "perc_popsrv","perc_popdep" "perc_popdep" FROM "fognat_com_serv" "fognat_com_serv"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FOGNAT_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FOGNAT_LOC_SERV_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FOGNAT_LOC_SERV'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_opera" "codice_opera","id_localita_istat" "id_localita_istat","perc_popsrv" "perc_popsrv","perc_popdep" "perc_popdep" FROM "fognat_loc_serv" "fognat_loc_serv"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FOGNAT_LOC_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FOGNATURE_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_FOGNATURE'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "a_fgn_rete_racc"."codice_ato" "codice_ato","a_fgn_rete_racc"."denom" "denom","fgn_auth_rete_racc"."n_scar_piena" "n_scar_piena","fgn_auth_rete_racc"."episodi_allag" "episodi_allag",TO_BIT("fgn_auth_rete_racc"."sn_strum_mis_port") "sn_strum_mis_port","fgn_auth_rete_racc"."d_telecont" "d_telecont","a_fgn_rete_racc"."d_stato" "d_stato","a_fgn_rete_racc"."data_agg" "data_agg","fgn_auth_rete_racc"."nr_rip" "nr_rip","fgn_auth_rete_racc"."nr_rip_allac" "nr_rip_allac","fgn_lunghezza_rete"."tipo_infr" "tipo_infr","fgn_lunghezza_rete"."lunghezza" "lunghezza","fgn_lunghezza_rete"."lunghezza_dep" "lunghezza_dep","fgn_lunghezza_allacci_id_rete"."lunghezza_allaccio" "lunghezza_allaccio","utenze_fognature_collettori"."nr_utenze_totali" "utenze_totali","utenze_fognature_collettori"."nr_utenze_industriali" "nr_utenze_industriali","utenze_fognature_collettori"."volume_utenze_totali" "volume_utenze_totali","utenze_fognature_collettori"."volume_utenze_industriali" "volume_utenze_industriali","fgn_vol_utenze"."vol_utenze_auth" "vol_utenze_auth","fgn_vol_utenze"."vol_fatturato" "vol_fatturato","fgn_lunghezza_rete"."lung_rete_mista" "lung_rete_mista","fgn_lunghezza_rete"."lung_rete_nera" "lung_rete_nera" FROM "a_fgn_rete_racc" "a_fgn_rete_racc" LEFT JOIN "fgn_auth_rete_racc" "fgn_auth_rete_racc" ON "fgn_auth_rete_racc"."id_rete_racc"="a_fgn_rete_racc"."idgis" LEFT JOIN "fgn_lunghezza_rete" "fgn_lunghezza_rete" ON "a_fgn_rete_racc"."idgis"="fgn_lunghezza_rete"."idgis" LEFT JOIN "abitanti_trattati" "abitanti_trattati" ON "a_fgn_rete_racc"."idgis"="abitanti_trattati"."idgis" LEFT JOIN "fgn_lunghezza_allacci_id_rete" "fgn_lunghezza_allacci_id_rete" ON "a_fgn_rete_racc"."idgis"="fgn_lunghezza_allacci_id_rete"."id_rete" LEFT JOIN "utenze_fognature_collettori" "utenze_fognature_collettori" ON "a_fgn_rete_racc"."idgis"="utenze_fognature_collettori"."id_rete" LEFT JOIN "fgn_vol_utenze" "fgn_vol_utenze" ON "a_fgn_rete_racc"."codice_ato"="fgn_vol_utenze"."ids_codice_orig_fgn" WHERE a_fgn_rete_racc.d_gestore = 'PUBLIACQUA' AND a_fgn_rete_racc.d_ambito IN ('AT3', NULL) AND a_fgn_rete_racc.d_stato NOT IN ('IPR', 'IAC') UNION ALL SELECT "fgn_rete_racc"."codice_ato" "codice_ato","fgn_rete_racc"."denom" "denom","fgn_auth_rete_racc"."n_scar_piena" "n_scar_piena","fgn_auth_rete_racc"."episodi_allag" "episodi_allag",TO_BIT("fgn_auth_rete_racc"."sn_strum_mis_port") "sn_strum_mis_port","fgn_auth_rete_racc"."d_telecont" "d_telecont","fgn_rete_racc"."d_stato" "d_stato","fgn_rete_racc"."data_agg" "data_agg","fgn_auth_rete_racc"."nr_rip" "nr_rip","fgn_auth_rete_racc"."nr_rip_allac" "nr_rip_allac","fgn_lunghezza_rete"."tipo_infr" "tipo_infr","fgn_lunghezza_rete"."lunghezza" "lunghezza","fgn_lunghezza_rete"."lunghezza_dep" "lunghezza_dep","fgn_lunghezza_allacci_id_rete"."lunghezza_allaccio" "lunghezza_allaccio","utenze_fognature_collettori"."nr_utenze_totali" "utenze_totali","utenze_fognature_collettori"."nr_utenze_industriali" "nr_utenze_industriali","utenze_fognature_collettori"."volume_utenze_totali" "volume_utenze_totali","utenze_fognature_collettori"."volume_utenze_industriali" "volume_utenze_industriali","fgn_vol_utenze"."vol_utenze_auth" "vol_utenze_auth","fgn_vol_utenze"."vol_fatturato" "vol_fatturato","fgn_lunghezza_rete"."lung_rete_mista" "lung_rete_mista","fgn_lunghezza_rete"."lung_rete_nera" "lung_rete_nera" FROM "fgn_rete_racc" "fgn_rete_racc" LEFT JOIN "fgn_auth_rete_racc" "fgn_auth_rete_racc" ON "fgn_auth_rete_racc"."id_rete_racc"="fgn_rete_racc"."idgis" LEFT JOIN "fgn_lunghezza_rete" "fgn_lunghezza_rete" ON "fgn_rete_racc"."idgis"="fgn_lunghezza_rete"."idgis" LEFT JOIN "fgn_lunghezza_allacci_id_rete" "fgn_lunghezza_allacci_id_rete" ON "fgn_rete_racc"."idgis"="fgn_lunghezza_allacci_id_rete"."id_rete" LEFT JOIN "utenze_fognature_collettori" "utenze_fognature_collettori" ON "fgn_rete_racc"."idgis"="utenze_fognature_collettori"."id_rete" LEFT JOIN "fgn_vol_utenze" "fgn_vol_utenze" ON "fgn_rete_racc"."codice_ato"="fgn_vol_utenze"."ids_codice_orig_fgn" WHERE fgn_rete_racc.d_gestore = 'PUBLIACQUA' AND fgn_rete_racc.d_ambito IN ('AT3', NULL) AND fgn_rete_racc.d_stato NOT IN ('IPR', 'IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_FOGNATURE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_LAGHI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_LAGHI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_captazione"."codice_ato" "codice_ato","acq_captazione"."denom" "denom",FROM_FLOAT_TO_INT("acq_captazione"."quota") "quota","acq_auth_capt"."bacino" "bacino","acq_captazione"."corso_acqua" "corso_acqua","acq_auth_capt"."d_classific" "d_classific","acq_capt_conces"."estremi_conces" "estremi_conces","acq_capt_conces"."port_potab" "port_potab","acq_captazione"."anno_costr" "anno_costr","acq_captazione"."anno_ristr" "anno_ristr","acq_captazione"."d_stato_cons" "d_stato_cons","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo","acq_auth_capt"."area_bac_affer" "area_bac_affer","acq_captazione"."volume_medio_prel" "volume_medio_prel","acq_capt_conces"."port_uti_max" "port_uti_max","acq_capt_conces"."port_uti_min" "port_uti_min","acq_capt_conces"."port_deriv" "port_deriv","acq_captazione"."volume_invaso" "volume_invaso","acq_captazione"."altezza_diga" "altezza_diga",TO_BIT("acq_auth_capt"."sn_inv_norm_coll") "sn_inv_norm_coll","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_captazione"."cod_comune" "cod_comune","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor","acq_captazione"."d_stato" "d_stato","acq_auth_capt"."a_area_bac_affer" "a_area_bac_affer","acq_captazione"."a_anno_costr" "a_anno_costr","acq_captazione"."a_anno_ristr" "a_anno_ristr","acq_captazione"."a_volume" "a_volume","acq_capt_conces"."a_port_es" "a_port_es","acq_capt_conces"."a_port_uti_max" "a_port_uti_max","acq_capt_conces"."a_port_uti_min" "a_port_uti_min","acq_capt_conces"."a_port_deriv" "a_port_deriv",TO_BIT("acq_capt_conces"."sn_uso_plurimo") "sn_uso_plurimo","acq_captazione"."data_agg" "data_agg",GB_X("acq_captazione"."geom") "transformed_x_geom",GB_Y("acq_captazione"."geom") "transformed_y_geom",IS_NULL("laghi_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","localita"."denominazi" "denominazi",TO_BIT("acq_auth_capt"."sn_tut_ass") "sn_tut_ass",TO_BIT("acq_auth_capt"."sn_rispetto") "sn_rispetto",TO_BIT("acq_auth_capt"."sn_protezione") "sn_protezione","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "acq_captazione" "acq_captazione" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "acq_captazione"."idgis"="acq_auth_capt"."id_captazione" LEFT JOIN "acq_capt_conces" "acq_capt_conces" ON "acq_captazione"."idgis"="acq_capt_conces"."id_captazione" LEFT JOIN "laghi_inpotab" "laghi_inpotab" ON "laghi_inpotab"."ids_codice"="acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_captazione"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "acq_captazione"."idgis"="schema_acq"."idgis" WHERE acq_captazione.d_gestore = 'PUBLIACQUA' AND acq_captazione.d_ambito IN ('AT3', NULL) AND acq_captazione.d_stato NOT IN ('IPR', 'IAC') AND SUB_FUNZIONE=1 UNION ALL SELECT "a_acq_captazione"."codice_ato" "codice_ato","a_acq_captazione"."denom" "denom",FROM_FLOAT_TO_INT("a_acq_captazione"."quota") "quota","acq_auth_capt"."bacino" "bacino","a_acq_captazione"."corso_acqua" "corso_acqua","acq_auth_capt"."d_classific" "d_classific","acq_capt_conces"."estremi_conces" "estremi_conces","acq_capt_conces"."port_potab" "port_potab","a_acq_captazione"."anno_costr" "anno_costr","a_acq_captazione"."anno_ristr" "anno_ristr","a_acq_captazione"."d_stato_cons" "d_stato_cons","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo","acq_auth_capt"."area_bac_affer" "area_bac_affer","a_acq_captazione"."volume_medio_prel" "volume_medio_prel","acq_capt_conces"."port_uti_max" "port_uti_max","acq_capt_conces"."port_uti_min" "port_uti_min","acq_capt_conces"."port_deriv" "port_deriv","a_acq_captazione"."volume_invaso" "volume_invaso","a_acq_captazione"."altezza_diga" "altezza_diga",TO_BIT("acq_auth_capt"."sn_inv_norm_coll") "sn_inv_norm_coll","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","a_acq_captazione"."cod_comune" "cod_comune","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor","a_acq_captazione"."d_stato" "d_stato","acq_auth_capt"."a_area_bac_affer" "a_area_bac_affer","a_acq_captazione"."a_anno_costr" "a_anno_costr","a_acq_captazione"."a_anno_ristr" "a_anno_ristr","a_acq_captazione"."a_volume" "a_volume","acq_capt_conces"."a_port_es" "a_port_es","acq_capt_conces"."a_port_uti_max" "a_port_uti_max","acq_capt_conces"."a_port_uti_min" "a_port_uti_min","acq_capt_conces"."a_port_deriv" "a_port_deriv",TO_BIT("acq_capt_conces"."sn_uso_plurimo") "sn_uso_plurimo","a_acq_captazione"."data_agg" "data_agg",GB_X("a_acq_captazione"."geom") "transformed_x_geom",GB_Y("a_acq_captazione"."geom") "transformed_y_geom",IS_NULL("laghi_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","localita"."denominazi" "denominazi",TO_BIT("acq_auth_capt"."sn_tut_ass") "sn_tut_ass",TO_BIT("acq_auth_capt"."sn_rispetto") "sn_rispetto",TO_BIT("acq_auth_capt"."sn_protezione") "sn_protezione","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "a_acq_captazione" "a_acq_captazione" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "a_acq_captazione"."idgis"="acq_auth_capt"."id_captazione" LEFT JOIN "acq_capt_conces" "acq_capt_conces" ON "a_acq_captazione"."idgis"="acq_capt_conces"."id_captazione" LEFT JOIN "laghi_inpotab" "laghi_inpotab" ON "laghi_inpotab"."ids_codice"="a_acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_captazione"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_captazione"."idgis"="schema_acq"."idgis" WHERE a_acq_captazione.d_gestore = 'PUBLIACQUA' AND a_acq_captazione.d_ambito IN ('AT3', NULL) AND a_acq_captazione.d_stato NOT IN ('IPR', 'IAC') AND SUB_FUNZIONE=1
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_LAGHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_LAGHI_INPOTAB_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_LAGHI_INPOTAB'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_potab" "ids_codice_potab","id_gestore_potab" "id_gestore_potab" FROM "laghi_inpotab" "laghi_inpotab"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_LAGHI_INPOTAB, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_LAGHI_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_LAGHI_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "laghi_inreti" "laghi_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_LAGHI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POMPAGGI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POMPAGGI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_pompaggio"."codice_ato" "codice_ato","acq_pompaggio"."denom" "denom",FROM_FLOAT_TO_INT("acq_pompaggio"."quota") "quota","acq_pompaggio"."anno_costr" "anno_costr","acq_auth_pompag"."anno_ristr_civ" "anno_ristr_civ","acq_auth_pompag"."d_stato_cons_civ" "d_stato_cons_civ","acq_auth_pompag"."anno_ristr_elmec" "anno_ristr_elmec","acq_auth_pompag"."d_stato_cons_elmec" "d_stato_cons_elmec","acq_auth_pompag"."d_telecont" "d_telecont","acq_pompaggio"."cod_comune" "cod_comune",TO_BIT("acq_auth_pompag"."sn_strum_mis_pres") "sn_strum_mis_pres",TO_BIT("acq_auth_pompag"."sn_strum_mis_port") "sn_strum_mis_port","acq_pompaggio"."d_stato" "d_stato","acq_pompaggio"."a_anno_costr" "a_anno_costr","acq_auth_pompag"."a_anno_ristr_civ" "a_anno_ristr_civ","acq_auth_pompag"."a_anno_ristr_elmec" "a_anno_ristr_elmec","acq_auth_pompag"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_pompag"."anno_instal_clor" "anno_instal_clor","acq_auth_pompag"."anno_ristr_clor" "anno_ristr_clor","acq_pompaggio"."data_agg" "data_agg",GB_X("acq_pompaggio"."geom") "transformed_x_geom",GB_Y("acq_pompaggio"."geom") "transformed_y_geom","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","localita"."denominazi" "denominazi","acq_auth_pompag"."consumo_ee" "consumo_ee","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "acq_pompaggio" "acq_pompaggio" LEFT JOIN "acq_auth_pompag" "acq_auth_pompag" ON "acq_auth_pompag"."id_pompaggio"="acq_pompaggio"."idgis" LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="acq_pompaggio"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_pompaggio"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "acq_pompaggio"."idgis"="schema_acq"."idgis" WHERE acq_pompaggio.d_gestore = 'PUBLIACQUA' AND acq_pompaggio.d_ambito IN ('AT3', NULL) AND acq_pompaggio.d_stato NOT IN ('IPR', 'IAC') UNION ALL SELECT "a_acq_pompaggio"."codice_ato" "codice_ato","a_acq_pompaggio"."denom" "denom",FROM_FLOAT_TO_INT("a_acq_pompaggio"."quota") "quota","a_acq_pompaggio"."anno_costr" "anno_costr","acq_auth_pompag"."anno_ristr_civ" "anno_ristr_civ","acq_auth_pompag"."d_stato_cons_civ" "d_stato_cons_civ","acq_auth_pompag"."anno_ristr_elmec" "anno_ristr_elmec","acq_auth_pompag"."d_stato_cons_elmec" "d_stato_cons_elmec","acq_auth_pompag"."d_telecont" "d_telecont","a_acq_pompaggio"."cod_comune" "cod_comune",TO_BIT("acq_auth_pompag"."sn_strum_mis_pres") "sn_strum_mis_pres",TO_BIT("acq_auth_pompag"."sn_strum_mis_port") "sn_strum_mis_port","a_acq_pompaggio"."d_stato" "d_stato","a_acq_pompaggio"."a_anno_costr" "a_anno_costr","acq_auth_pompag"."a_anno_ristr_civ" "a_anno_ristr_civ","acq_auth_pompag"."a_anno_ristr_elmec" "a_anno_ristr_elmec","acq_auth_pompag"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_pompag"."anno_instal_clor" "anno_instal_clor","acq_auth_pompag"."anno_ristr_clor" "anno_ristr_clor","a_acq_pompaggio"."data_agg" "data_agg",GB_X("a_acq_pompaggio"."geom") "transformed_x_geom",GB_Y("a_acq_pompaggio"."geom") "transformed_y_geom","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","localita"."denominazi" "denominazi","acq_auth_pompag"."consumo_ee" "consumo_ee","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "a_acq_pompaggio" "a_acq_pompaggio" LEFT JOIN "acq_auth_pompag" "acq_auth_pompag" ON "acq_auth_pompag"."id_pompaggio"="a_acq_pompaggio"."idgis" LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="a_acq_pompaggio"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_pompaggio"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_pompaggio"."idgis"="schema_acq"."idgis" WHERE a_acq_pompaggio.d_gestore = 'PUBLIACQUA' AND a_acq_pompaggio.d_ambito IN ('AT3', NULL) AND a_acq_pompaggio.d_stato NOT IN ('IPR', 'IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POMPAGGI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POMPAGGI_INPOTAB_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POMPAGGI_INPOTAB'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_potab" "ids_codice_potab","id_gestore_potab" "id_gestore_potab" FROM "pompaggi_inpotab" "pompaggi_inpotab"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POMPAGGI_INPOTAB, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POMPAGGI_INSERBA_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POMPAGGI_INSERBA'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_serbatoio" "ids_codice_serbatoio","id_gestore_serbatoio" "id_gestore_serbatoio" FROM "pompaggi_inserba" "pompaggi_inserba"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POMPAGGI_INSERBA, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POMPAGGI_POMPE_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POMPAGGI_POMPE'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_ato" "codice_ato","d_stato_cons" "d_stato_cons","anno_instal" "anno_instal","anno_ristr" "anno_ristr","potenza" "potenza","portata" "portata","prevalenza" "prevalenza","sn_riserva" "sn_riserva","idx_anno_instal" "a_anno_instal","idx_anno_ristr" "a_anno_ristr","idx_potenza" "a_potenza","idx_portata" "a_portata","idx_prevalenza" "a_prevalenza" FROM "pompaggi_pompe" "pompaggi_pompe"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POMPAGGI_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POTAB_INCAPTAZ_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POTAB_INCAPTAZ'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_captazione" "ids_codice_captazione","id_gestore_captazione" "id_gestore_captazione" FROM "potab_incaptaz" "potab_incaptaz"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POTAB_INCAPTAZ, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POTAB_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POTAB_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "potab_inreti" "potab_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POTAB_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POTAB_POMPE_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POTAB_POMPE'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_ato" "codice_ato","d_stato_cons" "d_stato_cons","anno_instal" "anno_instal","anno_ristr" "anno_ristr","potenza" "potenza","portata" "portata","prevalenza" "prevalenza","sn_riserva" "sn_riserva","idx_anno_instal" "idx_anno_instal","idx_anno_ristr" "idx_anno_ristr","idx_potenza" "idx_potenza","idx_portata" "idx_portata","idx_prevalenza" "idx_prevalenza" FROM "potab_pompe" "potab_pompe"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POTAB_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POTABILIZZATORI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POTABILIZZATORI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_auth_potab"."anno_ristr_civ" "anno_ristr_civ","acq_auth_potab"."d_stato_cons_civ" "d_stato_cons_civ","acq_auth_potab"."anno_ristr_elmec" "anno_ristr_elmec","acq_auth_potab"."d_stato_cons_elmec" "d_stato_cons_elmec","acq_auth_potab"."potenza_instal" "potenza_instal","acq_auth_potab"."presid_imp" "presid_imp",TO_BIT("acq_auth_potab"."sn_saltuario") "sn_saltuario","acq_auth_potab"."d_telecont" "d_telecont","acq_auth_potab"."riserva_acqua" "riserva_acqua",TO_BIT("acq_auth_potab"."sn_grigliat") "sn_grigliat",TO_BIT("acq_auth_potab"."sn_dissab") "sn_dissab",TO_BIT("acq_auth_potab"."sn_chiarific") "sn_chiarific","acq_auth_potab"."d_tipo_filtraz" "d_tipo_filtraz","acq_auth_potab"."d_ossid_riduz" "d_ossid_riduz",TO_BIT("acq_auth_potab"."sn_precipit") "sn_precipit",TO_BIT("acq_auth_potab"."sn_stripp") "sn_stripp","acq_auth_potab"."d_ass_carb_att" "d_ass_carb_att",TO_BIT("acq_auth_potab"."sn_res_sca_ion") "sn_res_sca_ion",TO_BIT("acq_auth_potab"."sn_elettrodial") "sn_elettrodial",TO_BIT("acq_auth_potab"."sn_osmosi_inv") "sn_osmosi_inv",TO_BIT("acq_auth_potab"."sn_filtr_lenta") "sn_filtr_lenta",TO_BIT("acq_auth_potab"."sn_tratt_bio") "sn_tratt_bio",TO_BIT("acq_auth_potab"."sn_mic_ult_filt") "sn_mic_ult_filt",TO_BIT("acq_auth_potab"."sn_acq_scar") "sn_acq_scar","acq_auth_potab"."d_tipo_cloraz" "d_tipo_cloraz",TO_BIT("acq_auth_potab"."sn_disinf_ozon") "sn_disinf_ozon",TO_BIT("acq_auth_potab"."sn_irrag_uv") "sn_irrag_uv",TO_BIT("acq_auth_potab"."sn_der_ferro") "sn_der_ferro",TO_BIT("acq_auth_potab"."sn_der_mangan") "sn_der_mangan",TO_BIT("acq_auth_potab"."sn_ammon_nitr") "sn_ammon_nitr",TO_BIT("acq_auth_potab"."sn_fosfati") "sn_fosfati",TO_BIT("acq_auth_potab"."sn_der_met_pes") "sn_der_met_pes",TO_BIT("acq_auth_potab"."sn_trialom") "sn_trialom",TO_BIT("acq_auth_potab"."sn_org_alog") "sn_org_alog",TO_BIT("acq_auth_potab"."sn_anidr_solf") "sn_anidr_solf","acq_auth_potab"."a_anno_ristr_civ" "a_anno_ristr_civ",TO_BIT("acq_auth_potab"."sn_strum_mis_port_in") "sn_strum_mis_port_in",TO_BIT("acq_auth_potab"."sn_strum_mis_port_out") "sn_strum_mis_port_out","acq_auth_potab"."vol_uscita" "vol_uscita","acq_auth_potab"."a_vol_uscita" "a_vol_uscita","acq_auth_potab"."a_anno_ristr_elmec" "a_anno_ristr_elmec","acq_auth_potab"."a_potenza_instal" "a_potenza_instal","acq_auth_potab"."consumo_ee" "consumo_ee","acq_potabiliz"."codice_ato" "codice_ato","acq_potabiliz"."denom" "denom",FROM_FLOAT_TO_INT("acq_potabiliz"."quota") "quota","acq_potabiliz"."d_tipo_trattam" "d_tipo_trattam","acq_potabiliz"."anno_costr" "anno_costr","acq_potabiliz"."vol_gg_trattabile" "vol_gg_trattabile","acq_potabiliz"."vol_anno_trattabile" "vol_anno_trattabile","acq_potabiliz"."d_stato" "d_stato","acq_potabiliz"."a_anno_costr" "a_anno_costr","acq_potabiliz"."a_vol_gg_tratt" "a_vol_gg_tratt","acq_potabiliz"."a_vol_anno_tratt" "a_vol_anno_tratt","acq_potabiliz"."data_agg" "data_agg","acq_potabiliz"."cod_comune" "cod_comune",GB_X("acq_potabiliz"."geom") "transformed_x_geom",GB_Y("acq_potabiliz"."geom") "transformed_y_geom","localita"."denominazi" "denominazi","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "acq_potabiliz" "acq_potabiliz" LEFT JOIN "acq_auth_potab" "acq_auth_potab" ON "acq_potabiliz"."idgis"="acq_auth_potab"."id_potabiliz" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_potabiliz"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "acq_potabiliz"."idgis"="schema_acq"."idgis" WHERE acq_potabiliz.d_gestore = 'PUBLIACQUA' AND acq_potabiliz.d_ambito IN ('AT3', NULL) AND acq_potabiliz.d_stato NOT IN ('IPR', 'IAC') UNION ALL SELECT "acq_auth_potab"."anno_ristr_civ" "anno_ristr_civ","acq_auth_potab"."d_stato_cons_civ" "d_stato_cons_civ","acq_auth_potab"."anno_ristr_elmec" "anno_ristr_elmec","acq_auth_potab"."d_stato_cons_elmec" "d_stato_cons_elmec","acq_auth_potab"."potenza_instal" "potenza_instal","acq_auth_potab"."presid_imp" "presid_imp",TO_BIT("acq_auth_potab"."sn_saltuario") "sn_saltuario","acq_auth_potab"."d_telecont" "d_telecont","acq_auth_potab"."riserva_acqua" "riserva_acqua",TO_BIT("acq_auth_potab"."sn_grigliat") "sn_grigliat",TO_BIT("acq_auth_potab"."sn_dissab") "sn_dissab",TO_BIT("acq_auth_potab"."sn_chiarific") "sn_chiarific","acq_auth_potab"."d_tipo_filtraz" "d_tipo_filtraz","acq_auth_potab"."d_ossid_riduz" "d_ossid_riduz",TO_BIT("acq_auth_potab"."sn_precipit") "sn_precipit",TO_BIT("acq_auth_potab"."sn_stripp") "sn_stripp","acq_auth_potab"."d_ass_carb_att" "d_ass_carb_att",TO_BIT("acq_auth_potab"."sn_res_sca_ion") "sn_res_sca_ion",TO_BIT("acq_auth_potab"."sn_elettrodial") "sn_elettrodial",TO_BIT("acq_auth_potab"."sn_osmosi_inv") "sn_osmosi_inv",TO_BIT("acq_auth_potab"."sn_filtr_lenta") "sn_filtr_lenta",TO_BIT("acq_auth_potab"."sn_tratt_bio") "sn_tratt_bio",TO_BIT("acq_auth_potab"."sn_mic_ult_filt") "sn_mic_ult_filt",TO_BIT("acq_auth_potab"."sn_acq_scar") "sn_acq_scar","acq_auth_potab"."d_tipo_cloraz" "d_tipo_cloraz",TO_BIT("acq_auth_potab"."sn_disinf_ozon") "sn_disinf_ozon",TO_BIT("acq_auth_potab"."sn_irrag_uv") "sn_irrag_uv",TO_BIT("acq_auth_potab"."sn_der_ferro") "sn_der_ferro",TO_BIT("acq_auth_potab"."sn_der_mangan") "sn_der_mangan",TO_BIT("acq_auth_potab"."sn_ammon_nitr") "sn_ammon_nitr",TO_BIT("acq_auth_potab"."sn_fosfati") "sn_fosfati",TO_BIT("acq_auth_potab"."sn_der_met_pes") "sn_der_met_pes",TO_BIT("acq_auth_potab"."sn_trialom") "sn_trialom",TO_BIT("acq_auth_potab"."sn_org_alog") "sn_org_alog",TO_BIT("acq_auth_potab"."sn_anidr_solf") "sn_anidr_solf","acq_auth_potab"."a_anno_ristr_civ" "a_anno_ristr_civ",TO_BIT("acq_auth_potab"."sn_strum_mis_port_in") "sn_strum_mis_port_in",TO_BIT("acq_auth_potab"."sn_strum_mis_port_out") "sn_strum_mis_port_out","acq_auth_potab"."vol_uscita" "vol_uscita","acq_auth_potab"."a_vol_uscita" "a_vol_uscita","acq_auth_potab"."a_anno_ristr_elmec" "a_anno_ristr_elmec","acq_auth_potab"."a_potenza_instal" "a_potenza_instal","acq_auth_potab"."consumo_ee" "consumo_ee","a_acq_potabiliz"."codice_ato" "codice_ato","a_acq_potabiliz"."denom" "denom",FROM_FLOAT_TO_INT("a_acq_potabiliz"."quota") "quota","a_acq_potabiliz"."d_tipo_trattam" "d_tipo_trattam","a_acq_potabiliz"."anno_costr" "anno_costr","a_acq_potabiliz"."vol_gg_trattabile" "vol_gg_trattabile","a_acq_potabiliz"."vol_anno_trattabile" "vol_anno_trattabile","a_acq_potabiliz"."d_stato" "d_stato","a_acq_potabiliz"."a_anno_costr" "a_anno_costr","a_acq_potabiliz"."a_vol_gg_tratt" "a_vol_gg_tratt","a_acq_potabiliz"."a_vol_anno_tratt" "a_vol_anno_tratt","a_acq_potabiliz"."data_agg" "data_agg","a_acq_potabiliz"."cod_comune" "cod_comune",GB_X("a_acq_potabiliz"."geom") "transformed_x_geom",GB_Y("a_acq_potabiliz"."geom") "transformed_y_geom","localita"."denominazi" "denominazi","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq" FROM "a_acq_potabiliz" "a_acq_potabiliz" LEFT JOIN "acq_auth_potab" "acq_auth_potab" ON "a_acq_potabiliz"."idgis"="acq_auth_potab"."id_potabiliz" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_potabiliz"."geom") LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_potabiliz"."idgis"="schema_acq"."idgis" WHERE a_acq_potabiliz.d_gestore = 'PUBLIACQUA' AND a_acq_potabiliz.d_ambito IN ('AT3', NULL) AND a_acq_potabiliz.d_stato NOT IN ('IPR', 'IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POTABILIZZATORI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POZZI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POZZI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_captazione"."codice_ato" "codice_ato","acq_captazione"."denom" "denom",FROM_FLOAT_TO_INT("acq_captazione"."quota") "quota","acq_capt_conces"."estremi_conces" "estremi_conces","acq_capt_conces"."port_potab" "port_potab","acq_captazione"."anno_costr" "anno_costr","acq_captazione"."anno_ristr" "anno_ristr","acq_captazione"."d_stato_cons" "d_stato_cons","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo","acq_captazione"."prof_pozzo" "prof_pozzo","acq_capt_conces"."diam_perf" "diam_perf","acq_captazione"."volume_medio_prel" "volume_medio_prel","acq_capt_conces"."port_uti_max" "port_uti_max","acq_capt_conces"."port_uti_min" "port_uti_min","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor",TO_BIT("acq_auth_capt"."sn_epis_inq") "sn_epis_inq","acq_captazione"."d_stato" "d_stato","acq_captazione"."a_anno_costr" "a_anno_costr","acq_captazione"."a_anno_ristr" "a_anno_ristr","acq_captazione"."a_volume" "a_volume","acq_capt_conces"."a_port_es" "a_port_es","acq_capt_conces"."a_port_uti_max" "a_port_uti_max","acq_capt_conces"."a_port_uti_min" "a_port_uti_min","acq_captazione"."data_agg" "data_agg","acq_captazione"."cod_comune" "cod_comune",GB_X("acq_captazione"."geom") "transformed_x_geom",GB_Y("acq_captazione"."geom") "transformed_y_geom",IS_NULL("pozzi_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","localita"."denominazi" "denominazi","acq_auth_capt"."consumo_ee" "consumo_ee",TO_BIT("acq_auth_capt"."sn_tut_ass") "sn_tut_ass",TO_BIT("acq_auth_capt"."sn_rispetto") "sn_rispetto",TO_BIT("acq_auth_capt"."sn_protezione") "sn_protezione","support_pozzi_inpotab"."volume_medio_prel" "support_vol_med_prel","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","support_codice_capt_accorp"."codice_accorp_capt" "codice_accorp_capt","support_codice_capt_accorp"."denom" "denom_accorp" FROM "acq_captazione" "acq_captazione" LEFT JOIN "acq_capt_conces" "acq_capt_conces" ON "acq_capt_conces"."id_captazione"="acq_captazione"."idgis" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "acq_auth_capt"."id_captazione"="acq_captazione"."idgis" LEFT JOIN "pozzi_inpotab" "pozzi_inpotab" ON "pozzi_inpotab"."ids_codice"="acq_captazione"."codice_ato" LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_captazione"."geom") LEFT JOIN "support_codice_capt_accorp" "support_codice_capt_accorp" ON "support_codice_capt_accorp"."idgis"="acq_captazione"."idgis" LEFT JOIN "support_pozzi_inpotab" "support_pozzi_inpotab" ON "support_pozzi_inpotab"."ids_codice"="acq_captazione"."codice_ato" LEFT JOIN "schema_acq" "schema_acq" ON "acq_captazione"."idgis"="schema_acq"."idgis" WHERE acq_captazione.d_gestore = 'PUBLIACQUA' AND acq_captazione.d_ambito IN ('AT3', NULL) AND acq_captazione.sub_funzione = 3 UNION ALL SELECT "a_acq_captazione"."codice_ato" "codice_ato","a_acq_captazione"."denom" "denom",FROM_FLOAT_TO_INT("a_acq_captazione"."quota") "quota","a_acq_capt_conces"."estremi_conces" "estremi_conces","a_acq_capt_conces"."port_potab" "port_potab","a_acq_captazione"."anno_costr" "anno_costr","a_acq_captazione"."anno_ristr" "anno_ristr","a_acq_captazione"."d_stato_cons" "d_stato_cons","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo","a_acq_captazione"."prof_pozzo" "prof_pozzo","a_acq_capt_conces"."diam_perf" "diam_perf","a_acq_captazione"."volume_medio_prel" "volume_medio_prel","a_acq_capt_conces"."port_uti_max" "port_uti_max","a_acq_capt_conces"."port_uti_min" "port_uti_min","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor",TO_BIT("acq_auth_capt"."sn_epis_inq") "sn_epis_inq","a_acq_captazione"."d_stato" "d_stato","a_acq_captazione"."a_anno_costr" "a_anno_costr","a_acq_captazione"."a_anno_ristr" "a_anno_ristr","a_acq_captazione"."a_volume" "a_volume","a_acq_capt_conces"."a_port_es" "a_port_es","a_acq_capt_conces"."a_port_uti_max" "a_port_uti_max","a_acq_capt_conces"."a_port_uti_min" "a_port_uti_min","a_acq_captazione"."data_agg" "data_agg","a_acq_captazione"."cod_comune" "cod_comune",GB_X("a_acq_captazione"."geom") "transformed_x_geom",GB_Y("a_acq_captazione"."geom") "transformed_y_geom",IS_NULL("pozzi_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","localita"."denominazi" "denominazi","acq_auth_capt"."consumo_ee" "consumo_ee",TO_BIT("acq_auth_capt"."sn_tut_ass") "sn_tut_ass",TO_BIT("acq_auth_capt"."sn_rispetto") "sn_rispetto",TO_BIT("acq_auth_capt"."sn_protezione") "sn_protezione","support_pozzi_inpotab"."volume_medio_prel" "support_vol_med_prel","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","support_codice_capt_accorp"."codice_accorp_capt" "codice_accorp_capt","support_codice_capt_accorp"."denom" "denom_accorp" FROM "a_acq_captazione" "a_acq_captazione" LEFT JOIN "a_acq_capt_conces" "a_acq_capt_conces" ON "a_acq_capt_conces"."id_captazione"="a_acq_captazione"."idgis" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "acq_auth_capt"."id_captazione"="a_acq_captazione"."idgis" LEFT JOIN "pozzi_inpotab" "pozzi_inpotab" ON "pozzi_inpotab"."ids_codice"="a_acq_captazione"."codice_ato" LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="a_acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_captazione"."geom") LEFT JOIN "support_codice_capt_accorp" "support_codice_capt_accorp" ON "support_codice_capt_accorp"."idgis"="a_acq_captazione"."idgis" LEFT JOIN "support_pozzi_inpotab" "support_pozzi_inpotab" ON "support_pozzi_inpotab"."ids_codice"="a_acq_captazione"."codice_ato" LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_captazione"."idgis"="schema_acq"."idgis" WHERE a_acq_captazione.d_gestore = 'PUBLIACQUA' AND a_acq_captazione.d_ambito IN ('AT3', NULL) AND a_acq_captazione.sub_funzione = 3
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POZZI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POZZI_INPOTAB_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POZZI_INPOTAB'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_potab" "ids_codice_potab","id_gestore_potab" "id_gestore_potab" FROM "pozzi_inpotab" "pozzi_inpotab"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POZZI_INPOTAB, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POZZI_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POZZI_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "pozzi_inreti" "pozzi_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POZZI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POZZI_POMPE_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_POZZI_POMPE'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_ato" "codice_ato","d_stato_cons" "d_stato_cons","anno_instal" "anno_instal","anno_ristr" "anno_ristr","potenza" "potenza","portata" "portata","prevalenza" "prevalenza","sn_riserva" "sn_riserva","idx_anno_instal" "idx_anno_instal","idx_anno_ristr" "idx_anno_ristr","idx_potenza" "idx_potenza","idx_portata" "idx_portata","idx_prevalenza" "idx_prevalenza" FROM "pozzi_pompe" "pozzi_pompe"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_POZZI_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SCARICAT_INFOG_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SCARICAT_INFOG'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_fognatura" "ids_codice_fognatura","id_gestore_fognatura" "id_gestore_fognatura" FROM "scaricato_infog" "scaricato_infog"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SCARICAT_INFOG, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SCARICATORI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SCARICATORI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "fgn_sfioro"."codice_ato" "codice_ato","fgn_sfioro"."denom" "denom",FROM_FLOAT_TO_INT("fgn_auth_sfioro"."quota_terr_man") "quota_terr_man","fgn_sfioro"."d_tipo_scolm" "d_tipo_scolm","fgn_sfioro"."anno_costr" "anno_costr","fgn_sfioro"."anno_ristr" "anno_ristr","fgn_sfioro"."d_materiale" "d_materiale","fgn_auth_sfioro"."larg_uti_poz_scol" "larg_uti_poz_scol","fgn_auth_sfioro"."lung_uti_poz_scol" "lung_uti_poz_scol","fgn_sfioro"."rapporto_diluiz" "rapporto_diluiz",TO_BIT("fgn_auth_sfioro"."sn_strum_mis_port") "sn_strum_mis_port","fgn_auth_sfioro"."tipo_recapito" "tipo_recapito","fgn_auth_sfioro"."bacino_recet" "bacino_recet","fgn_sfioro"."d_stato_cons" "d_stato_cons","fgn_auth_sfioro"."d_telecont" "d_telecont","fgn_sfioro"."d_stato" "d_stato","fgn_sfioro"."a_anno_costr" "a_anno_costr","fgn_sfioro"."a_anno_ristr" "a_anno_ristr","fgn_sfioro"."data_agg" "data_agg","fgn_sfioro"."d_classif" "d_classif","fgn_sfioro"."cod_comune" "cod_comune",GB_X("fgn_sfioro"."geom") "transformed_x_geom",GB_Y("fgn_sfioro"."geom") "transformed_y_geom","localita"."denominazi" "denominazi",TO_BIT("fgn_sfioro"."sn_bypass") "sn_bypass" FROM "fgn_sfioro" "fgn_sfioro" LEFT JOIN "fgn_auth_sfioro" "fgn_auth_sfioro" ON "fgn_auth_sfioro"."id_sfioro"="fgn_sfioro"."idgis" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","fgn_sfioro"."geom") WHERE fgn_sfioro.d_gestore = 'PUBLIACQUA' AND fgn_sfioro.d_ambito IN ('AT3', NULL) AND fgn_sfioro.d_stato NOT IN ('IPR', 'IAC') UNION ALL SELECT "a_fgn_sfioro"."codice_ato" "codice_ato","a_fgn_sfioro"."denom" "denom",FROM_FLOAT_TO_INT("fgn_auth_sfioro"."quota_terr_man") "quota_terr_man","a_fgn_sfioro"."d_tipo_scolm" "d_tipo_scolm","a_fgn_sfioro"."anno_costr" "anno_costr","a_fgn_sfioro"."anno_ristr" "anno_ristr","a_fgn_sfioro"."d_materiale" "d_materiale","fgn_auth_sfioro"."larg_uti_poz_scol" "larg_uti_poz_scol","fgn_auth_sfioro"."lung_uti_poz_scol" "lung_uti_poz_scol","a_fgn_sfioro"."rapporto_diluiz" "rapporto_diluiz",TO_BIT("fgn_auth_sfioro"."sn_strum_mis_port") "sn_strum_mis_port","fgn_auth_sfioro"."tipo_recapito" "tipo_recapito","fgn_auth_sfioro"."bacino_recet" "bacino_recet","a_fgn_sfioro"."d_stato_cons" "d_stato_cons","fgn_auth_sfioro"."d_telecont" "d_telecont","a_fgn_sfioro"."d_stato" "d_stato","a_fgn_sfioro"."a_anno_costr" "a_anno_costr","a_fgn_sfioro"."a_anno_ristr" "a_anno_ristr","a_fgn_sfioro"."data_agg" "data_agg","a_fgn_sfioro"."d_classif" "d_classif","a_fgn_sfioro"."cod_comune" "cod_comune",GB_X("a_fgn_sfioro"."geom") "transformed_x_geom",GB_Y("a_fgn_sfioro"."geom") "transformed_y_geom","localita"."denominazi" "denominazi",TO_BIT("a_fgn_sfioro"."sn_bypass") "sn_bypass" FROM "a_fgn_sfioro" "a_fgn_sfioro" LEFT JOIN "fgn_auth_sfioro" "fgn_auth_sfioro" ON "fgn_auth_sfioro"."id_sfioro"="a_fgn_sfioro"."idgis" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_fgn_sfioro"."geom") WHERE a_fgn_sfioro.d_gestore = 'PUBLIACQUA' AND a_fgn_sfioro.d_ambito IN ('AT3', NULL) AND a_fgn_sfioro.d_stato NOT IN ('IPR', 'IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SCARICATORI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SOLLEV_POMPE_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SOLLEV_POMPE'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "codice_ato" "codice_ato","d_stato_cons" "d_stato_cons","anno_instal" "anno_instal","anno_ristr" "anno_ristr","potenza" "potenza","portata" "portata","prevalenza" "prevalenza","sn_riserva" "sn_riserva","idx_anno_instal" "idx_anno_instal","idx_anno_ristr" "idx_anno_ristr","idx_potenza" "idx_potenza","idx_portata" "idx_portata","idx_prevalenza" "idx_prevalenza" FROM "sollev_pompe" "sollev_pompe"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SOLLEV_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SOLLEVAMENTI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SOLLEVAMENTI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "fgn_imp_sollev"."codice_ato" "codice_ato","fgn_imp_sollev"."denom" "denom",FROM_FLOAT_TO_INT("fgn_imp_sollev"."quota") "quota","fgn_imp_sollev"."anno_costr" "anno_costr","fgn_auth_imp_sol"."anno_ristr_civ" "anno_ristr_civ","fgn_auth_imp_sol"."d_stato_cons_civ" "d_stato_cons_civ","fgn_auth_imp_sol"."anno_ristr_elmec" "anno_ristr_elmec","fgn_auth_imp_sol"."d_stato_cons_elmec" "d_stato_cons_elmec",TO_BIT("fgn_imp_sollev"."sn_sgrigliat") "sn_sgrigliat","fgn_auth_imp_sol"."d_telecont" "d_telecont",TO_BIT("fgn_auth_imp_sol"."sn_strum_mis_pres") "sn_strum_mis_pres",TO_BIT("fgn_auth_imp_sol"."sn_strum_mis_port") "sn_strum_mis_port","fgn_imp_sollev"."d_stato" "d_stato","fgn_imp_sollev"."a_anno_costr" "a_anno_costr","fgn_auth_imp_sol"."a_anno_ristr_civ" "a_anno_ristr_civ","fgn_auth_imp_sol"."a_anno_ristr_elmec" "a_anno_ristr_elmec","fgn_imp_sollev"."data_agg" "data_agg","fgn_imp_sollev"."cod_comune" "cod_comune",GB_X("fgn_imp_sollev"."geom") "transformed_x_geom",GB_Y("fgn_imp_sollev"."geom") "transformed_y_geom","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","localita"."denominazi" "denominazi","fgn_auth_imp_sol"."consumo_ee" "consumo_ee" FROM "fgn_imp_sollev" "fgn_imp_sollev" LEFT JOIN "fgn_auth_imp_sol" "fgn_auth_imp_sol" ON "fgn_auth_imp_sol"."id_imp_sollev"="fgn_imp_sollev"."idgis" LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="fgn_imp_sollev"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","fgn_imp_sollev"."geom") WHERE fgn_imp_sollev.d_gestore = 'PUBLIACQUA' AND fgn_imp_sollev.d_ambito IN ('AT3', NULL) AND fgn_imp_sollev.d_stato NOT IN ('IPR', 'IAC') UNION ALL SELECT "a_fgn_imp_sollev"."codice_ato" "codice_ato","a_fgn_imp_sollev"."denom" "denom",FROM_FLOAT_TO_INT("a_fgn_imp_sollev"."quota") "quota","a_fgn_imp_sollev"."anno_costr" "anno_costr","fgn_auth_imp_sol"."anno_ristr_civ" "anno_ristr_civ","fgn_auth_imp_sol"."d_stato_cons_civ" "d_stato_cons_civ","fgn_auth_imp_sol"."anno_ristr_elmec" "anno_ristr_elmec","fgn_auth_imp_sol"."d_stato_cons_elmec" "d_stato_cons_elmec",TO_BIT("a_fgn_imp_sollev"."sn_sgrigliat") "sn_sgrigliat","fgn_auth_imp_sol"."d_telecont" "d_telecont",TO_BIT("fgn_auth_imp_sol"."sn_strum_mis_pres") "sn_strum_mis_pres",TO_BIT("fgn_auth_imp_sol"."sn_strum_mis_port") "sn_strum_mis_port","a_fgn_imp_sollev"."d_stato" "d_stato","a_fgn_imp_sollev"."a_anno_costr" "a_anno_costr","fgn_auth_imp_sol"."a_anno_ristr_civ" "a_anno_ristr_civ","fgn_auth_imp_sol"."a_anno_ristr_elmec" "a_anno_ristr_elmec","a_fgn_imp_sollev"."data_agg" "data_agg","a_fgn_imp_sollev"."cod_comune" "cod_comune",GB_X("a_fgn_imp_sollev"."geom") "transformed_x_geom",GB_Y("a_fgn_imp_sollev"."geom") "transformed_y_geom","stats_pompe"."sum_potenza" "sum_potenza","stats_pompe"."avg_idx_potenza" "avg_idx_potenza","localita"."denominazi" "denominazi","fgn_auth_imp_sol"."consumo_ee" "consumo_ee" FROM "a_fgn_imp_sollev" "a_fgn_imp_sollev" LEFT JOIN "fgn_auth_imp_sol" "fgn_auth_imp_sol" ON "fgn_auth_imp_sol"."id_imp_sollev"="a_fgn_imp_sollev"."idgis" LEFT JOIN "stats_pompe" "stats_pompe" ON "stats_pompe"."codice_ato"="a_fgn_imp_sollev"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_fgn_imp_sollev"."geom") WHERE a_fgn_imp_sollev.d_gestore = 'PUBLIACQUA' AND a_fgn_imp_sollev.d_ambito IN ('AT3', NULL) AND a_fgn_imp_sollev.d_stato NOT IN ('IPR', 'IAC')
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SOLLEVAMENTI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SORGENTI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SORGENTI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "acq_captazione"."codice_ato" "codice_ato","acq_captazione"."denom" "denom",FROM_FLOAT_TO_INT("acq_captazione"."quota") "quota","acq_capt_conces"."estremi_conces" "estremi_conces","acq_capt_conces"."port_potab" "port_potab","acq_captazione"."anno_costr" "anno_costr","acq_captazione"."anno_ristr" "anno_ristr","acq_captazione"."d_stato_cons" "d_stato_cons","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo",TO_BIT("acq_auth_capt"."sn_cunic_presa") "sn_cunic_presa",TO_BIT("acq_auth_capt"."sn_vasca_capt") "sn_vasca_capt",TO_BIT("acq_auth_capt"."sn_vasca_mis") "sn_vasca_mis",TO_BIT("acq_auth_capt"."sn_vasca_car") "sn_vasca_car","acq_captazione"."volume_medio_prel" "volume_medio_prel","acq_capt_conces"."port_uti_max" "port_uti_max","acq_capt_conces"."port_uti_min" "port_uti_min","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor",TO_BIT("acq_auth_capt"."sn_epis_inq") "sn_epis_inq","acq_captazione"."d_stato" "d_stato","acq_captazione"."a_anno_costr" "a_anno_costr","acq_captazione"."a_anno_ristr" "a_anno_ristr","acq_captazione"."a_volume" "a_volume","acq_capt_conces"."a_port_es" "a_port_es","acq_capt_conces"."a_port_uti_max" "a_port_uti_max","acq_capt_conces"."a_port_uti_min" "a_port_uti_min","acq_captazione"."data_agg" "data_agg","acq_captazione"."cod_comune" "cod_comune",GB_X("acq_captazione"."geom") "transformed_x_geom",GB_Y("acq_captazione"."geom") "transformed_y_geom",IS_NULL("sorgenti_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","localita"."denominazi" "denominazi","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","support_codice_capt_accorp"."codice_accorp_capt" "codice_accorp_capt","support_codice_capt_accorp"."denom" "denom_accorp" FROM "acq_captazione" "acq_captazione" LEFT JOIN "acq_capt_conces" "acq_capt_conces" ON "acq_capt_conces"."id_captazione"="acq_captazione"."idgis" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "acq_auth_capt"."id_captazione"="acq_captazione"."idgis" LEFT JOIN "sorgenti_inpotab" "sorgenti_inpotab" ON "sorgenti_inpotab"."ids_codice"="acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","acq_captazione"."geom") LEFT JOIN "support_codice_capt_accorp" "support_codice_capt_accorp" ON "acq_captazione"."idgis"="support_codice_capt_accorp"."idgis" LEFT JOIN "schema_acq" "schema_acq" ON "acq_captazione"."idgis"="schema_acq"."idgis" WHERE acq_captazione.d_gestore = 'PUBLIACQUA' AND acq_captazione.d_ambito IN ('AT3', NULL) AND acq_captazione.d_stato NOT IN ('IPR', 'IAC') AND acq_captazione.sub_funzione = 4 UNION ALL SELECT "a_acq_captazione"."codice_ato" "codice_ato","a_acq_captazione"."denom" "denom",FROM_FLOAT_TO_INT("a_acq_captazione"."quota") "quota","a_acq_capt_conces"."estremi_conces" "estremi_conces","a_acq_capt_conces"."port_potab" "port_potab","a_acq_captazione"."anno_costr" "anno_costr","a_acq_captazione"."anno_ristr" "anno_ristr","a_acq_captazione"."d_stato_cons" "d_stato_cons","acq_auth_capt"."d_utilizzo" "d_utilizzo","acq_auth_capt"."utilizzo_annuo" "utilizzo_annuo",TO_BIT("acq_auth_capt"."sn_cunic_presa") "sn_cunic_presa",TO_BIT("acq_auth_capt"."sn_vasca_capt") "sn_vasca_capt",TO_BIT("acq_auth_capt"."sn_vasca_mis") "sn_vasca_mis",TO_BIT("acq_auth_capt"."sn_vasca_car") "sn_vasca_car","a_acq_captazione"."volume_medio_prel" "volume_medio_prel","a_acq_capt_conces"."port_uti_max" "port_uti_max","a_acq_capt_conces"."port_uti_min" "port_uti_min","acq_auth_capt"."d_telecont" "d_telecont",TO_BIT("acq_auth_capt"."sn_strum_mis_port") "sn_strum_mis_port","acq_auth_capt"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_capt"."anno_instal_clor" "anno_instal_clor","acq_auth_capt"."anno_ristr_clor" "anno_ristr_clor",TO_BIT("acq_auth_capt"."sn_epis_inq") "sn_epis_inq","a_acq_captazione"."d_stato" "d_stato","a_acq_captazione"."a_anno_costr" "a_anno_costr","a_acq_captazione"."a_anno_ristr" "a_anno_ristr","a_acq_captazione"."a_volume" "a_volume","a_acq_capt_conces"."a_port_es" "a_port_es","a_acq_capt_conces"."a_port_uti_max" "a_port_uti_max","a_acq_capt_conces"."a_port_uti_min" "a_port_uti_min","a_acq_captazione"."data_agg" "data_agg","a_acq_captazione"."cod_comune" "cod_comune",GB_X("a_acq_captazione"."geom") "transformed_x_geom",GB_Y("a_acq_captazione"."geom") "transformed_y_geom",IS_NULL("sorgenti_inpotab"."ids_codice") "cod_opera_inpotab_not_exist","localita"."denominazi" "denominazi","schema_acq"."codice_schema_acq" "codice_schema_acq","schema_acq"."denominazione_schema_acq" "denominazione_schema_acq","support_codice_capt_accorp"."codice_accorp_capt" "codice_accorp_capt","support_codice_capt_accorp"."denom" "denom_accorp" FROM "a_acq_captazione" "a_acq_captazione" LEFT JOIN "a_acq_capt_conces" "a_acq_capt_conces" ON "a_acq_capt_conces"."id_captazione"="a_acq_captazione"."idgis" LEFT JOIN "acq_auth_capt" "acq_auth_capt" ON "acq_auth_capt"."id_captazione"="a_acq_captazione"."idgis" LEFT JOIN "sorgenti_inpotab" "sorgenti_inpotab" ON "sorgenti_inpotab"."ids_codice"="a_acq_captazione"."codice_ato" LEFT JOIN "localita" "localita" ON ST_INTERSECTS("localita"."geom","a_acq_captazione"."geom") LEFT JOIN "support_codice_capt_accorp" "support_codice_capt_accorp" ON "a_acq_captazione"."idgis"="support_codice_capt_accorp"."idgis" LEFT JOIN "schema_acq" "schema_acq" ON "a_acq_captazione"."idgis"="schema_acq"."idgis" WHERE a_acq_captazione.d_gestore = 'PUBLIACQUA' AND a_acq_captazione.d_ambito IN ('AT3', NULL) AND a_acq_captazione.d_stato NOT IN ('IPR', 'IAC') AND a_acq_captazione.sub_funzione = 4
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SORGENTI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SORGENTI_INPOTAB_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SORGENTI_INPOTAB'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_potab" "ids_codice_potab","id_gestore_potab" "id_gestore_potab" FROM "sorgenti_inpotab" "sorgenti_inpotab"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SORGENTI_INPOTAB, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SORGENTI_INRETI_xls() returns void as $$ 
DECLARE 
   v_count BIGINT:=0; 
   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_SORGENTI_INRETI'); 
BEGIN 
   SET search_path = public,dbiait_analysis; 
   SELECT count(0) INTO v_count FROM ( 
   SELECT "ids_codice" "ids_codice","ids_codice_rete" "ids_codice_rete","id_gestore_rete" "id_gestore_rete" FROM "sorgenti_inreti" "sorgenti_inreti"
   ) t; 
   SET search_path = public,pgunit; 
   PERFORM test_assertTrue('count XLS_SORGENTI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); 
END; 
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; 
--------------------------------------------------------------------------------
