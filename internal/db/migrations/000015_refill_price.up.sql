-- The refill price the route prober measured for the peer of an auto-rebalance
-- channel: the fee ppm of the cheapest route that carried a probe back to us
-- over the peer, at the largest amount that got through. NULL ppm with a check
-- time: no probe reached us down to AR-MinAmount.
ALTER TABLE gui_channels
    ADD COLUMN refill_ppm        integer,
    ADD COLUMN refill_amount_sat bigint,
    ADD COLUMN refill_checked    timestamp with time zone;
