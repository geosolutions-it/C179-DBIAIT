from app.shape_checks.tasks.shape_checks_base_task import ShapeChecksBaseTask


class ShpAcqCheckTask(ShapeChecksBaseTask):
    """
    Dramatiq SHP_ACQ check task class.
    """
    sheet_for_dbf = "SHP_Acquedotto"

class ShpFgnCheckTask(ShapeChecksBaseTask):
    """
    Dramatiq SHP_FFN check task definition class.
    """
    sheet_for_dbf = "SHP_Fognatura"