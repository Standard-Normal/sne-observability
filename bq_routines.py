NODE = """
    merge `frontoffice-291900.SNE_NEW_MODEL.sn_node` as tgt
    using
    (
        select * from (
            select * 
            from EXTERNAL_QUERY(
                "projects/frontoffice-291900/locations/us/connections/postgreSQL", 
                "SELECT * FROM new_model.node;")
        )
    ) src
    ON tgt.node_id = src.node_id
    WHEN MATCHED THEN
        UPDATE set
        tgt.market_id = src.market_id,
        tgt.node_name = src.node_name,
        tgt.node_desc = src.node_desc,
        tgt.node_type = src.node_type,
        tgt.node_alias = src.node_alias,
        tgt.external_node_id = src.external_node_id,
        tgt.parent_node_id = src.parent_node_id,
        tgt.deenergized = src.deenergized
    WHEN NOT MATCHED THEN
        insert
        (node_id, market_id, node_name, node_desc, node_type, node_alias, external_node_id, parent_node_id, deenergized)
        values
        (src.node_id, src.market_id, src.node_name, src.node_desc, src.node_type, src.node_alias, src.external_node_id, src.parent_node_id, src.deenergized)
    ;
"""

NYISO_SOLAR = """
    merge `frontoffice-291900.SNE_NEW_MODEL.sn_nyiso_actual_solar_fcst` as tgt
    using
    (
        select * from (
            select * 
            from EXTERNAL_QUERY(
                "projects/frontoffice-291900/locations/us/connections/postgreSQL", 
                "SELECT * FROM new_model.sn_nyiso_actual_solar_fcst where opr_date >= current_date - interval '3 day';")
        )
    ) src
    ON tgt.nyiso_ld_gen_id = src.nyiso_ld_gen_id
    WHEN MATCHED THEN
        UPDATE set
        tgt.opr_date = src.opr_date,
        tgt.opr_hour = src.opr_hour,
        tgt.capitl_actual_solar_mwh = src.capitl_actual_solar_mwh,
        tgt.centrl_actual_solar_mwh = src.centrl_actual_solar_mwh,
        tgt.dunwod_actual_solar_mwh = src.dunwod_actual_solar_mwh,
        tgt.genese_actual_solar_mwh = src.genese_actual_solar_mwh,
        tgt.hudvl_actual_solar_mwh = src.hudvl_actual_solar_mwh,
        tgt.longil_actual_solar_mwh = src.longil_actual_solar_mwh,
        tgt.mhkvl_actual_solar_mwh = src.mhkvl_actual_solar_mwh,
        tgt.millwd_actual_solar_mwh = src.millwd_actual_solar_mwh,
        tgt.nyc_actual_solar_mwh = src.nyc_actual_solar_mwh,
        tgt.north_actual_solar_mwh = src.north_actual_solar_mwh,
        tgt.west_actual_solar_mwh = src.west_actual_solar_mwh,
        tgt.nyiso_actual_solar_mwh = src.nyiso_actual_solar_mwh,
        tgt.capitl_fcst_solar_mwh = src.capitl_fcst_solar_mwh,
        tgt.centrl_fcst_solar_mwh = src.centrl_fcst_solar_mwh,
        tgt.dunwod_fcst_solar_mwh = src.dunwod_fcst_solar_mwh,
        tgt.genese_fcst_solar_mwh = src.genese_fcst_solar_mwh,
        tgt.hudvl_fcst_solar_mwh = src.hudvl_fcst_solar_mwh,
        tgt.longil_fcst_solar_mwh = src.longil_fcst_solar_mwh,
        tgt.mhkvl_fcst_solar_mwh = src.mhkvl_fcst_solar_mwh,
        tgt.millwd_fcst_solar_mwh = src.millwd_fcst_solar_mwh,
        tgt.nyc_fcst_solar_mwh = src.nyc_fcst_solar_mwh,
        tgt.north_fcst_solar_mwh = src.north_fcst_solar_mwh,
        tgt.west_fcst_solar_mwh = src.west_fcst_solar_mwh,
        tgt.nyiso_fcst_solar_mwh = src.nyiso_fcst_solar_mwh
    WHEN NOT MATCHED THEN
        insert
        (nyiso_ld_gen_id, opr_date, opr_hour, capitl_actual_solar_mwh, centrl_actual_solar_mwh, dunwod_actual_solar_mwh, genese_actual_solar_mwh, hudvl_actual_solar_mwh, longil_actual_solar_mwh, mhkvl_actual_solar_mwh, millwd_actual_solar_mwh, nyc_actual_solar_mwh, north_actual_solar_mwh, west_actual_solar_mwh, nyiso_actual_solar_mwh, capitl_fcst_solar_mwh, centrl_fcst_solar_mwh, dunwod_fcst_solar_mwh, genese_fcst_solar_mwh, hudvl_fcst_solar_mwh, longil_fcst_solar_mwh, mhkvl_fcst_solar_mwh, millwd_fcst_solar_mwh, nyc_fcst_solar_mwh, north_fcst_solar_mwh, west_fcst_solar_mwh, nyiso_fcst_solar_mwh)
        values
        (src.nyiso_ld_gen_id, src.opr_date, src.opr_hour, src.capitl_actual_solar_mwh, src.centrl_actual_solar_mwh, src.dunwod_actual_solar_mwh, src.genese_actual_solar_mwh, src.hudvl_actual_solar_mwh, src.longil_actual_solar_mwh, src.mhkvl_actual_solar_mwh, src.millwd_actual_solar_mwh, src.nyc_actual_solar_mwh, src.north_actual_solar_mwh, src.west_actual_solar_mwh, src.nyiso_actual_solar_mwh, src.capitl_fcst_solar_mwh, src.centrl_fcst_solar_mwh, src.dunwod_fcst_solar_mwh, src.genese_fcst_solar_mwh, src.hudvl_fcst_solar_mwh, src.longil_fcst_solar_mwh, src.mhkvl_fcst_solar_mwh, src.millwd_fcst_solar_mwh, src.nyc_fcst_solar_mwh, src.north_fcst_solar_mwh, src.west_fcst_solar_mwh, src.nyiso_fcst_solar_mwh)
    ;
"""

NYISO_LOAD = """
    merge `frontoffice-291900.SNE_NEW_MODEL.sn_nyiso_actual_load_fcst` as tgt
    using
    (
        select * from (
            select * 
            from EXTERNAL_QUERY(
                "projects/frontoffice-291900/locations/us/connections/postgreSQL", 
                "SELECT * FROM new_model.sn_nyiso_actual_load_fcst where opr_date >= current_date - interval '3 day';")
        )
    ) src
    ON tgt.nyiso_ld_gen_id = src.nyiso_ld_gen_id
    WHEN MATCHED THEN
        UPDATE set
        tgt.opr_date = src.opr_date,
        tgt.opr_hour = src.opr_hour,
        tgt.capitl_actual_load_mwh = src.capitl_actual_load_mwh,
        tgt.centrl_actual_load_mwh = src.centrl_actual_load_mwh,
        tgt.dunwod_actual_load_mwh = src.dunwod_actual_load_mwh,
        tgt.genese_actual_load_mwh = src.genese_actual_load_mwh,
        tgt.hudvl_actual_load_mwh = src.hudvl_actual_load_mwh,
        tgt.longil_actual_load_mwh = src.longil_actual_load_mwh,
        tgt.mhkvl_actual_load_mwh = src.mhkvl_actual_load_mwh,
        tgt.millwd_actual_load_mwh = src.millwd_actual_load_mwh,
        tgt.nyc_actual_load_mwh = src.nyc_actual_load_mwh,
        tgt.north_actual_load_mwh = src.north_actual_load_mwh,
        tgt.west_actual_load_mwh = src.west_actual_load_mwh,
        tgt.nyiso_actual_load_mwh = src.nyiso_actual_load_mwh,
        tgt.capitl_fcst_load_mwh = src.capitl_fcst_load_mwh,
        tgt.centrl_fcst_load_mwh = src.centrl_fcst_load_mwh,
        tgt.dunwod_fcst_load_mwh = src.dunwod_fcst_load_mwh,
        tgt.genese_fcst_load_mwh = src.genese_fcst_load_mwh,
        tgt.hudvl_fcst_load_mwh = src.hudvl_fcst_load_mwh,
        tgt.longil_fcst_load_mwh = src.longil_fcst_load_mwh,
        tgt.mhkvl_fcst_load_mwh = src.mhkvl_fcst_load_mwh,
        tgt.millwd_fcst_load_mwh = src.millwd_fcst_load_mwh,
        tgt.nyc_fcst_load_mwh = src.nyc_fcst_load_mwh,
        tgt.north_fcst_load_mwh = src.north_fcst_load_mwh,
        tgt.west_fcst_load_mwh = src.west_fcst_load_mwh,
        tgt.nyiso_fcst_load_mwh = src.nyiso_fcst_load_mwh
    WHEN NOT MATCHED THEN
        insert
        (nyiso_ld_gen_id, opr_date, opr_hour, capitl_actual_load_mwh, centrl_actual_load_mwh, dunwod_actual_load_mwh, genese_actual_load_mwh, hudvl_actual_load_mwh, longil_actual_load_mwh, mhkvl_actual_load_mwh, millwd_actual_load_mwh, nyc_actual_load_mwh, north_actual_load_mwh, west_actual_load_mwh, nyiso_actual_load_mwh, capitl_fcst_load_mwh, centrl_fcst_load_mwh, dunwod_fcst_load_mwh, genese_fcst_load_mwh, hudvl_fcst_load_mwh, longil_fcst_load_mwh, mhkvl_fcst_load_mwh, millwd_fcst_load_mwh, nyc_fcst_load_mwh, north_fcst_load_mwh, west_fcst_load_mwh, nyiso_fcst_load_mwh)
        values
        (src.nyiso_ld_gen_id, src.opr_date, src.opr_hour, src.capitl_actual_load_mwh, src.centrl_actual_load_mwh, src.dunwod_actual_load_mwh, src.genese_actual_load_mwh, src.hudvl_actual_load_mwh, src.longil_actual_load_mwh, src.mhkvl_actual_load_mwh, src.millwd_actual_load_mwh, src.nyc_actual_load_mwh, src.north_actual_load_mwh, src.west_actual_load_mwh, src.nyiso_actual_load_mwh, src.capitl_fcst_load_mwh, src.centrl_fcst_load_mwh, src.dunwod_fcst_load_mwh, src.genese_fcst_load_mwh, src.hudvl_fcst_load_mwh, src.longil_fcst_load_mwh, src.mhkvl_fcst_load_mwh, src.millwd_fcst_load_mwh, src.nyc_fcst_load_mwh, src.north_fcst_load_mwh, src.west_fcst_load_mwh, src.nyiso_fcst_load_mwh)
    ;
"""

NYISO_LMP = """
    merge `frontoffice-291900.SNE_NEW_MODEL.sn_nyiso_lmp` as tgt
    using
    (
        select * from (
            select * 
            from EXTERNAL_QUERY(
                "projects/frontoffice-291900/locations/us/connections/postgreSQL", 
                "SELECT * FROM new_model.sn_dart_lmp where market_id=3 and opr_date >= current_date - interval '3 day';")
        )
    ) src
    ON tgt.dart_id = src.dart_id
    WHEN MATCHED THEN
        UPDATE set
        tgt.opr_date = src.opr_date,
        tgt.opr_hour = src.opr_hour,
        tgt.market_id = src.market_id,
        tgt.node_id = src.node_id,
        tgt.da_lmp = src.da_lmp,
        tgt.da_mec = src.da_mec,
        tgt.da_mlc = src.da_mlc,
        tgt.da_mcc = src.da_mcc,
        tgt.rt_lmp = src.rt_lmp,
        tgt.rt_mec = src.rt_mec,
        tgt.rt_mlc = src.rt_mlc,
        tgt.rt_mcc = src.rt_mcc,
        tgt.dart_lmp = src.dart_lmp,
        tgt.created_by_user = src.created_by_user,
        tgt.created_ts = src.created_ts,
        tgt.updated_by_user = src.updated_by_user,
        tgt.update_ts = src.update_ts,
        tgt.da_mghg = src.da_mghg,
        tgt.rt_mghg = src.rt_mghg
    WHEN NOT MATCHED THEN
        insert
        (dart_id, opr_date, opr_hour, market_id, node_id, da_lmp, da_mec, da_mlc, da_mcc, rt_lmp, rt_mec, rt_mlc, rt_mcc, dart_lmp, created_by_user, created_ts, updated_by_user, update_ts, da_mghg, rt_mghg)
        values
        (src.dart_id, src.opr_date, src.opr_hour, src.market_id, src.node_id, src.da_lmp, src.da_mec, src.da_mlc, src.da_mcc, src.rt_lmp, src.rt_mec, src.rt_mlc, src.rt_mcc, src.dart_lmp, src.created_by_user, src.created_ts, src.updated_by_user, src.update_ts, src.da_mghg, src.rt_mghg)
    ;
"""


ERCOT_SOLAR = """
"""

ERCOT_WIND = """
"""

ERCOT_LOAD = """
"""

ERCOT_LMP = """
    merge `frontoffice-291900.SNE_NEW_MODEL.sn_ercot_lmp` as tgt
    using
    (
        select * from (
            select * 
            from EXTERNAL_QUERY(
                "projects/frontoffice-291900/locations/us/connections/postgreSQL", 
                "SELECT * FROM new_model.sn_dart_lmp where market_id=1 and opr_date >= current_date - interval '3 day';")
        )
    ) src
    ON tgt.dart_id = src.dart_id
    WHEN MATCHED THEN
        UPDATE set
        tgt.opr_date = src.opr_date,
        tgt.opr_hour = src.opr_hour,
        tgt.market_id = src.market_id,
        tgt.node_id = src.node_id,
        tgt.da_lmp = src.da_lmp,
        tgt.da_mec = src.da_mec,
        tgt.da_mlc = src.da_mlc,
        tgt.da_mcc = src.da_mcc,
        tgt.rt_lmp = src.rt_lmp,
        tgt.rt_mec = src.rt_mec,
        tgt.rt_mlc = src.rt_mlc,
        tgt.rt_mcc = src.rt_mcc,
        tgt.dart_lmp = src.dart_lmp,
        tgt.created_by_user = src.created_by_user,
        tgt.created_ts = src.created_ts,
        tgt.updated_by_user = src.updated_by_user,
        tgt.update_ts = src.update_ts,
        tgt.da_mghg = src.da_mghg,
        tgt.rt_mghg = src.rt_mghg
    WHEN NOT MATCHED THEN
        insert
        (dart_id, opr_date, opr_hour, market_id, node_id, da_lmp, da_mec, da_mlc, da_mcc, rt_lmp, rt_mec, rt_mlc, rt_mcc, dart_lmp, created_by_user, created_ts, updated_by_user, update_ts, da_mghg, rt_mghg)
        values
        (src.dart_id, src.opr_date, src.opr_hour, src.market_id, src.node_id, src.da_lmp, src.da_mec, src.da_mlc, src.da_mcc, src.rt_lmp, src.rt_mec, src.rt_mlc, src.rt_mcc, src.dart_lmp, src.created_by_user, src.created_ts, src.updated_by_user, src.update_ts, src.da_mghg, src.rt_mghg)
    ;
"""
