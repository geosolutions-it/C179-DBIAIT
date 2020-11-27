# ACCUMULI SHEET EXPECTED QUERIES

expected_accumuli_query = """SELECT "acq_accumulo"."codice_ato" "codice_ato","acq_accumulo"."denom" "denom","acq_accumulo"."quota" "quota","acq_accumulo"."anno_costr" "anno_costr","acq_accumulo"."anno_ristr" "anno_ristr","acq_accumulo"."d_stato_cons" "d_stato_cons","acq_accumulo"."d_ubicazione" "d_ubicazione","acq_accumulo"."d_materiale" "d_materiale","acq_accumulo"."volume" "volume","acq_accumulo"."quota_fondo" "quota_fondo","acq_accumulo"."d_stato" "d_stato","acq_accumulo"."a_anno_costr" "a_anno_costr","acq_accumulo"."a_anno_ristr" "a_anno_ristr","acq_accumulo"."a_volume" "a_volume","acq_accumulo"."data_agg" "data_agg","acq_accumulo"."cod_comune" "cod_comune",GB_X("acq_accumulo"."geom") "transformed_x_geom",GB_Y("acq_accumulo"."geom") "transformed_y_geom","acq_auth_accum"."d_telecont" "d_telecont","acq_auth_accum"."sn_strum_mis_liv" "sn_strum_mis_liv","acq_auth_accum"."sn_strum_mis_port" "sn_strum_mis_port","acq_auth_accum"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_accum"."anno_instal_clor" "anno_instal_clor","acq_auth_accum"."anno_ristr_clor" "anno_ristr_clor" FROM "acq_accumulo" "acq_accumulo" JOIN "acq_auth_accum" "acq_auth_accum" ON "acq_accumulo"."idgis"="acq_auth_accum"."id_accumulo" WHERE acq_accumulo.d_gestore = 'PUBLIACQUA' AND acq_accumulo.d_ambito IN ('AT3', NULL) AND acq_accumulo.d_stato NOT IN ('IPR','IAC')"""
expected_a_accumuli_query = """SELECT "a_acq_accumulo"."codice_ato" "codice_ato","a_acq_accumulo"."denom" "denom","a_acq_accumulo"."quota" "quota","a_acq_accumulo"."anno_costr" "anno_costr","a_acq_accumulo"."anno_ristr" "anno_ristr","a_acq_accumulo"."d_stato_cons" "d_stato_cons","a_acq_accumulo"."d_ubicazione" "d_ubicazione","a_acq_accumulo"."d_materiale" "d_materiale","a_acq_accumulo"."volume" "volume","a_acq_accumulo"."quota_fondo" "quota_fondo","a_acq_accumulo"."d_stato" "d_stato","a_acq_accumulo"."a_anno_costr" "a_anno_costr","a_acq_accumulo"."a_anno_ristr" "a_anno_ristr","a_acq_accumulo"."a_volume" "a_volume","a_acq_accumulo"."data_agg" "data_agg","a_acq_accumulo"."cod_comune" "cod_comune",GB_X("a_acq_accumulo"."geom") "transformed_x_geom",GB_Y("a_acq_accumulo"."geom") "transformed_y_geom","acq_auth_accum"."d_telecont" "d_telecont","acq_auth_accum"."sn_strum_mis_liv" "sn_strum_mis_liv","acq_auth_accum"."sn_strum_mis_port" "sn_strum_mis_port","acq_auth_accum"."d_tipo_cloraz" "d_tipo_cloraz","acq_auth_accum"."anno_instal_clor" "anno_instal_clor","acq_auth_accum"."anno_ristr_clor" "anno_ristr_clor" FROM "a_acq_accumulo" "a_acq_accumulo" JOIN "acq_auth_accum" "acq_auth_accum" ON "a_acq_accumulo"."idgis"="acq_auth_accum"."id_accumulo" WHERE a_acq_accumulo.d_gestore = 'PUBLIACQUA' AND a_acq_accumulo.d_ambito IN ('AT3', NULL) AND a_acq_accumulo.d_stato NOT IN ('IPR','IAC')"""
expected_accumuli_spatial_query = '''SELECT "localita"."denominazione" "denominazione" FROM "acq_accumulo" "acq_accumulo" SPATIAL JOIN "localita" "localita" ON "acq_accumulo"."shape"="localita"."shape"'''

# ADDUT TRONCHI SHEET EXPECTED QUERIES

expected_addut_tronchi_query = '''SELECT "codice_ato" "codice_ato","idgis" "idgis","idgis_rete" "idgis_rete","id_tipo_telecon" "id_tipo_telecon","id_materiale" "id_materiale","id_conservazione" "id_conservazione","diametro" "diametro","anno" "anno","lunghezza" "lunghezza","idx_materiale" "idx_materiale","idx_diametro" "idx_diametro","idx_anno" "idx_anno","idx_lunghezza" "idx_lunghezza","pressione" "pressione","protezione_catodica" "protezione_catodica","note" "note","geom" "geom" FROM "addut_tronchi" "addut_tronchi"'''
expected_addut_tronchi_condotta_query = '''SELECT "acq_adduttrice"."codice_ato" "codice_ato","acq_condotta"."id_sist_prot_cat" "id_sist_prot_cat","acq_condotta"."data_esercizio" "data_esercizio" FROM "acq_condotta" "acq_condotta" JOIN "acq_adduttrice" "acq_adduttrice" ON "acq_condotta"."id_rete"="acq_adduttrice"."idgis"'''

# ADDUTTRICI SHEET EXPECTED QUERIES

expected_adduttrici_query = """"""
expected_a_addutrici_query = """"""










