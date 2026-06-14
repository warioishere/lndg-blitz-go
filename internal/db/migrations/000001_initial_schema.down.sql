-- 000001_initial_schema.down.sql
--
-- Rollback des initialen Schemas. CASCADE loest die FK-Constraints
-- (gui_allowedtarget -> gui_channels, gui_paymenthops -> gui_payments).

DROP TABLE IF EXISTS gui_allowedtarget CASCADE;
DROP TABLE IF EXISTS gui_ambosspeerfees CASCADE;
DROP TABLE IF EXISTS gui_autofees CASCADE;
DROP TABLE IF EXISTS gui_autopilot CASCADE;
DROP TABLE IF EXISTS gui_avoidnodes CASCADE;
DROP TABLE IF EXISTS gui_channels CASCADE;
DROP TABLE IF EXISTS gui_closures CASCADE;
DROP TABLE IF EXISTS gui_failedhtlcs CASCADE;
DROP TABLE IF EXISTS gui_forwards CASCADE;
DROP TABLE IF EXISTS gui_graphevent CASCADE;
DROP TABLE IF EXISTS gui_graphprobelog CASCADE;
DROP TABLE IF EXISTS gui_histfailedhtlc CASCADE;
DROP TABLE IF EXISTS gui_inboundfeelog CASCADE;
DROP TABLE IF EXISTS gui_invoices CASCADE;
DROP TABLE IF EXISTS gui_localsettings CASCADE;
DROP TABLE IF EXISTS gui_nodecache CASCADE;
DROP TABLE IF EXISTS gui_nodereputation CASCADE;
DROP TABLE IF EXISTS gui_onchain CASCADE;
DROP TABLE IF EXISTS gui_paymenthops CASCADE;
DROP TABLE IF EXISTS gui_payments CASCADE;
DROP TABLE IF EXISTS gui_peerevents CASCADE;
DROP TABLE IF EXISTS gui_peers CASCADE;
DROP TABLE IF EXISTS gui_pendingchannels CASCADE;
DROP TABLE IF EXISTS gui_pendinghtlcs CASCADE;
DROP TABLE IF EXISTS gui_probelog CASCADE;
DROP TABLE IF EXISTS gui_rebalancer CASCADE;
DROP TABLE IF EXISTS gui_rebalanceroute CASCADE;
DROP TABLE IF EXISTS gui_resolutions CASCADE;
DROP TABLE IF EXISTS gui_tradesales CASCADE;
