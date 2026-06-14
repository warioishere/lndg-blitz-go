package web

import (
	"net/http"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleApiIncome returns all-time revenue statistics. GetInfo is called
// unconditionally; an RPC failure returns an error response. Integer truncation
// towards zero (int()) is used for ppm calculations to match the original behavior.
func (s *Server) handleApiIncome(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if _, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{}); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+grpcErrorMsg(err))
		return
	}

	var forwardCount, forwardSumOut int64
	var forwardSumFee float64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(amt_out_msat),0), COALESCE(sum(fee),0) FROM gui_forwards`).
		Scan(&forwardCount, &forwardSumOut, &forwardSumFee); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var invoiceCount, invoiceSumPaid int64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(amt_paid),0) FROM gui_invoices WHERE state=1 AND is_revenue=true`).
		Scan(&invoiceCount, &invoiceSumPaid); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var paymentCount int64
	var paymentSumValue, paymentSumFee float64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(value),0), COALESCE(sum(fee),0) FROM gui_payments WHERE status=2`).
		Scan(&paymentCount, &paymentSumValue, &paymentSumFee); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var onchainCount, onchainSumFee int64
	if err := s.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(fee),0) FROM gui_onchain`).
		Scan(&onchainCount, &onchainSumFee); err != nil {
		writeAPIError(w, "Failed to get revenue stats! Error: "+err.Error())
		return
	}

	var closuresSum, closuresCount int64
	if err := s.db.QueryRow(ctx,
		`SELECT COALESCE(sum(closing_costs),0), count(*) FROM gui_closures`).
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
