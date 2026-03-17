-- Documented Master Task pointing to the Orchestrator SP
CREATE OR REPLACE TASK TSK_BRONZE_MASTER
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '60 MINUTE'
AS
    CALL RAW.SP_BRONZE_ORCHESTRATOR();

-- Enable the schedule immediately
ALTER TASK TSK_BRONZE_MASTER RESUME;

