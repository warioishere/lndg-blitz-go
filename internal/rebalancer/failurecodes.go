package rebalancer

import "strconv"

// failureCodeNames maps numeric failure codes to their human-readable names.
var failureCodeNames = map[int]string{
	1:  "INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS",
	2:  "INCORRECT_PAYMENT_AMOUNT",
	3:  "FINAL_INCORRECT_CLTV_EXPIRY",
	4:  "FINAL_INCORRECT_HTLC_AMOUNT",
	5:  "FINAL_EXPIRY_TOO_SOON",
	6:  "INVALID_REALM",
	7:  "EXPIRY_TOO_SOON",
	8:  "INVALID_ONION_VERSION",
	9:  "INVALID_ONION_HMAC",
	10: "INVALID_ONION_KEY",
	11: "AMOUNT_BELOW_MINIMUM",
	12: "FEE_INSUFFICIENT",
	13: "INCORRECT_CLTV_EXPIRY",
	14: "CHANNEL_DISABLED",
	15: "TEMPORARY_CHANNEL_FAILURE",
	16: "REQUIRED_NODE_FEATURE_MISSING",
	17: "REQUIRED_CHANNEL_FEATURE_MISSING",
	18: "UNKNOWN_NEXT_PEER",
	19: "TEMPORARY_NODE_FAILURE",
	20: "PERMANENT_NODE_FAILURE",
	21: "PERMANENT_CHANNEL_FAILURE",
	22: "EXPIRY_TOO_FAR",
	23: "MPP_TIMEOUT",
}

// failureDetailNames maps numeric failure detail codes to their human-readable names.
var failureDetailNames = map[int]string{
	0:  "UNKNOWN",
	1:  "NO_DETAIL",
	2:  "ONION_DECODE",
	3:  "LINK_NOT_ELIGIBLE",
	4:  "ON_CHAIN_TIMEOUT",
	5:  "HTLC_EXCEEDS_MAX",
	6:  "INSUFFICIENT_BALANCE",
	7:  "INCOMPLETE_FORWARD",
	8:  "HTLC_ADD_FAILED",
	9:  "FORWARDS_DISABLED",
	10: "INVOICE_CANCELED",
	11: "INVOICE_UNDERPAID",
	12: "INVOICE_EXPIRY_TOO_SOON",
	13: "INVOICE_NOT_OPEN",
	14: "MPP_INVOICE_TIMEOUT",
	15: "ADDRESS_MISMATCH",
	16: "SET_TOTAL_MISMATCH",
	17: "SET_TOTAL_TOO_LOW",
	18: "SET_OVERPAID",
	19: "UNKNOWN_INVOICE",
	20: "INVALID_KEYSEND",
	21: "MPP_IN_PROGRESS",
	22: "CIRCULAR_ROUTE",
}

// failureCodeName returns the name for a failure code, or the code as a string if unknown.
func failureCodeName(code int) string {
	if name, ok := failureCodeNames[code]; ok {
		return name
	}
	return strconv.Itoa(code)
}

// failureDetailName returns the name for a failure detail, or the value as a string if unknown.
func failureDetailName(detail int) string {
	if name, ok := failureDetailNames[detail]; ok {
		return name
	}
	return strconv.Itoa(detail)
}
