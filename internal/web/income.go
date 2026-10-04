package web

import (
	"net/http"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleApiIncome returns revenue statistics, all-time or for the last N days
// with "?=N" (Python parses the query string minus its first character).
// Integer truncation towards zero (int()) is used for ppm calculations to match
// the original behavior.
func (s *Server) handleApiIncome(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+grpcErrorMsg(err))
		return
	}
	// $1 = date cutoff, $2 = close_height cutoff; NULL = no filter
	var dayFilter, heightFilter any
	if raw := r.URL.RawQuery; raw != "" {
		if days, err := strconv.Atoi(raw[1:]); err == nil && days != 0 {
			dayFilter = time.Now().UTC().AddDate(0, 0, -days)
			heightFilter = int64(info.GetBlockHeight()) - int64(days)*144
		}
	}

	var forwardCount, forwardSumOut int64
	var forwardSumFee float64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(amt_out_msat),0), COALESCE(sum(fee),0) FROM gui_forwards WHERE ($1::timestamptz IS NULL OR forward_date >= $1)`, dayFilter).
		Scan(&forwardCount, &forwardSumOut, &forwardSumFee); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var invoiceCount, invoiceSumPaid int64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(amt_paid),0) FROM gui_invoices WHERE state=1 AND is_revenue=true AND ($1::timestamptz IS NULL OR settle_date >= $1)`, dayFilter).
		Scan(&invoiceCount, &invoiceSumPaid); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var paymentCount int64
	var paymentSumValue, paymentSumFee float64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(value),0), COALESCE(sum(fee),0) FROM gui_payments WHERE status=2 AND ($1::timestamptz IS NULL OR creation_date >= $1)`, dayFilter).
		Scan(&paymentCount, &paymentSumValue, &paymentSumFee); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var onchainCount, onchainSumFee int64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(fee),0) FROM gui_onchain WHERE ($1::timestamptz IS NULL OR time_stamp >= $1)`, dayFilter).
		Scan(&onchainCount, &onchainSumFee); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var closuresSum, closuresCount int64
	if err := s.db.QueryRow(ctx,
		`SELECT COALESCE(sum(closing_costs),0), count(*) FROM gui_closures WHERE ($1::bigint IS NULL OR close_height >= $1)`, heightFilter).
		Scan(&closuresSum, &closuresCount); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	// Integer conversion truncates towards zero (equivalent to int()).
	var forwardAmount, totalRevenue int64
	if forwardCount != 0 {
		forwardAmount = int64(float64(forwardSumOut) / 1000)
		totalRevenue = int64(forwardSumFee)
	}
	var totalReceived int64
	if invoiceCount != 0 {
		totalReceived = invoiceSumPaid
	}
	totalRevenue += totalReceived

	var totalRevenuePpm int64
	if forwardAmount != 0 {
		totalRevenuePpm = int64(float64(totalRevenue) / (float64(forwardAmount) / 1000000))
	}

	var totalSent, totalFees int64
	if paymentCount != 0 {
		totalSent = int64(paymentSumValue)
		totalFees = int64(paymentSumFee)
	}
	var totalFeesPpm int64
	if totalSent != 0 {
		totalFeesPpm = int64(float64(totalFees) / (float64(totalSent) / 1000000))
	}

	var onchainCosts int64
	if onchainCount != 0 {
		onchainCosts = onchainSumFee
	}
	if closuresCount != 0 {
		onchainCosts += closuresSum
	}

	profits := totalRevenue - totalFees - onchainCosts
	var profitsPpm int64
	if forwardAmount != 0 {
		profitsPpm = int64(float64(profits) / (float64(forwardAmount) / 1000000))
	}
	var percentCost int64
	if totalRevenue != 0 {
		percentCost = int64((float64(totalFees+onchainCosts) / float64(totalRevenue)) * 100)
	}

	data := newOrderedMap().
		Set("forward_count", forwardCount).
		Set("forward_amount", forwardAmount).
		Set("total_revenue", totalRevenue).
		Set("total_revenue_ppm", totalRevenuePpm).
		Set("total_fees", totalFees).
		Set("total_fees_ppm", totalFeesPpm).
		Set("onchain_costs", onchainCosts).
		Set("profits", profits).
		Set("profits_ppm", profitsPpm).
		Set("percent_cost", percentCost)
	writeSuccess(w, data)
}
