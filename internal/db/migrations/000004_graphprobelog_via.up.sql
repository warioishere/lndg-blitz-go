-- Results of the graph watcher's forced-route probe through the new peer: the
-- buy price it was gated on, the verified route's cost and amount, and the
-- gui_rebalancer status of the purchase over that route (NULL: no purchase).
ALTER TABLE gui_graphprobelog
    ADD COLUMN buy_ppm integer,
    ADD COLUMN via_fee_ppm integer,
    ADD COLUMN via_amount_sat bigint,
    ADD COLUMN buy_status integer;
