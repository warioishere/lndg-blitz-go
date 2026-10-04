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

// failureCodeName returns the name for a failure code, or the code as a string if unknown.
func failureCodeName(code int) string {
	if name, ok := failureCodeNames[code]; ok {
		return name
	}
	return strconv.Itoa(code)
}
