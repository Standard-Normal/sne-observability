NODES_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_node`
 (
    node_id int64,
	market_id int64,
	node_name string,
	node_desc string,
	node_type string,
	node_alias string,
	external_node_id string,
	parent_node_id int64,
	deenergized bool,
 )
 OPTIONS(
   description="nyiso wind actual and forecast data",
   labels=[("market", "all"), ("variable", "nodes")]
 );
 """

NYISO_SOLAR_TABLE = """
    CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_nyiso_actual_solar_fcst`
    (
        nyiso_ld_gen_id int64,
        opr_date datetime,
        opr_hour int64,
        capitl_actual_solar_mwh float64,
        centrl_actual_solar_mwh float64,
        dunwod_actual_solar_mwh float64,
        genese_actual_solar_mwh float64,
        hudvl_actual_solar_mwh float64,
        longil_actual_solar_mwh float64,
        mhkvl_actual_solar_mwh float64,
        millwd_actual_solar_mwh float64,
        nyc_actual_solar_mwh float64,
        north_actual_solar_mwh float64,
        west_actual_solar_mwh float64,
        nyiso_actual_solar_mwh float64,
        capitl_fcst_solar_mwh float64,
        centrl_fcst_solar_mwh float64,
        dunwod_fcst_solar_mwh float64,
        genese_fcst_solar_mwh float64,
        hudvl_fcst_solar_mwh float64,
        longil_fcst_solar_mwh float64,
        mhkvl_fcst_solar_mwh float64,
        millwd_fcst_solar_mwh float64,
        nyc_fcst_solar_mwh float64,
        north_fcst_solar_mwh float64,
        west_fcst_solar_mwh float64,
        nyiso_fcst_solar_mwh float64
    )
    OPTIONS(
    description="nyiso solar actual and forecast data",
    labels=[("market", "nyiso"), ("variable", "solar")]
    );
 """

NYISO_LOAD_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_nyiso_actual_load_fcst`
 (
    nyiso_ld_gen_id int64,
	opr_date datetime,
	opr_hour int64,
	capitl_actual_load_mwh float64,
	centrl_actual_load_mwh float64,
	dunwod_actual_load_mwh float64,
	genese_actual_load_mwh float64,
	hudvl_actual_load_mwh float64,
	longil_actual_load_mwh float64,
	mhkvl_actual_load_mwh float64,
	millwd_actual_load_mwh float64,
	nyc_actual_load_mwh float64,
	north_actual_load_mwh float64,
	west_actual_load_mwh float64,
	nyiso_actual_load_mwh float64,
	capitl_fcst_load_mwh float64,
	centrl_fcst_load_mwh float64,
	dunwod_fcst_load_mwh float64,
	genese_fcst_load_mwh float64,
	hudvl_fcst_load_mwh float64,
	longil_fcst_load_mwh float64,
	mhkvl_fcst_load_mwh float64,
	millwd_fcst_load_mwh float64,
	nyc_fcst_load_mwh float64,
	north_fcst_load_mwh float64,
	west_fcst_load_mwh float64,
	nyiso_fcst_load_mwh float64
 )
 OPTIONS(
   description="nyiso load actual and forecast data",
   labels=[("market", "nyiso"), ("variable", "load")]
 );
"""

NYISO_LMP_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_nyiso_lmp`
 (
    dart_id int64,
	opr_date datetime,
	opr_hour int64,
	market_id int64,
	node_id int64,
	da_lmp float64,
	da_mec float64,
	da_mlc float64,
	da_mcc float64,
	rt_lmp float64,
	rt_mec float64,
	rt_mlc float64,
	rt_mcc float64,
	dart_lmp float64,
	created_by_user string,
	created_ts datetime,
	updated_by_user string,
	update_ts datetime,
	da_mghg float64,
	rt_mghg float64
 )
 OPTIONS(
   description="nyiso da and rt data",
   labels=[("market", "nyiso"), ("variable", "lmp")]
 );
"""


ERCOT_SOLAR_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_ercot_solar`
 (
    erct_slr_gen_id int64,
	opr_date datetime,
	opr_hour int64,
	actual_solar_gen float64,
	stppf float64,
	pvgrpp float64
 )
 OPTIONS(
   description="ercot solar actual and forecast data",
   labels=[("market", "ercot"), ("variable", "solar")]
 );
"""

ERCOT_WIND_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_ercot_wind`
 (
    erct_wnd_gen_id int64,
	opr_date datetime,
	opr_hour int64,
	coastal_actual float64,
	north_actual float64,
	panhandle_actual float64,
	south_actual float64,
	west_actual float64,
	ercot_actual float64,
	coastal_fcst float64,
	north_fcst float64,
	panhandle_fcst float64,
	south_fcst float64,
	west_fcst float64,
	ercot_fcst float64
 )
 OPTIONS(
   description="ercot wind actual and forecast data",
   labels=[("market", "ercot"), ("variable", "wind")]
 );
"""

ERCOT_LOAD_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_ercot_load`
 (
    erct_ld_gen_id int64,
	opr_date datetime,
	opr_hour int64,
	dst int64,
	coast_actual_load_mwh float64,
	east_actual_load_mwh float64,
	ercot_actual_load_mwh float64,
	farwest_actual_load_mwh float64,
	north_actual_load_mwh float64,
	northcentral_actual_load_mwh float64,
	southcentral_actual_load_mwh float64,
	southern_actual_load_mwh float64,
	west_actual_load_mwh float64,
	coast_fcst_load_mwh float64,
	east_fcst_load_mwh float64,
	ercot_fcst_load_mwh float64,
	farwest_fcst_load_mwh float64,
	north_fcst_load_mwh float64,
	northcentral_fcst_load_mwh float64,
	southcentral_fcst_load_mwh float64,
	southern_fcst_load_mwh float64,
	west_fcst_load_mwh float64
 )
 OPTIONS(
   description="ercot load actual and forecast data",
   labels=[("market", "ercot"), ("variable", "load")]
 );
"""



ERCOT_LMP_TABLE = """
CREATE TABLE IF NOT EXISTS `frontoffice-291900.SNE_NEW_MODEL.sn_ercot_lmp`
 (
    dart_id int64,
	opr_date datetime,
	opr_hour int64,
	market_id int64,
	node_id int64,
	da_lmp float64,
	da_mec float64,
	da_mlc float64,
	da_mcc float64,
	rt_lmp float64,
	rt_mec float64,
	rt_mlc float64,
	rt_mcc float64,
	dart_lmp float64,
	created_by_user string,
	created_ts datetime,
	updated_by_user string,
	update_ts datetime,
	da_mghg float64,
	rt_mghg float64
 )
 OPTIONS(
   description="nyiso da and rt data",
   labels=[("market", "ercot"), ("variable", "lmp")]
 );
"""
