package metadata

import (
	"strconv"
	"strings"
	"unsafe"
)

// FromByte converts bytes to a string without memory allocation.
// NOTE: The given bytes MUST NOT be modified since they share the same backing array
// with the returned string.
func FromByte(b []byte) string {
	// Ignore if your IDE shows an error here; it's a false positive.
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func detectCSVType(val string) string {
	s := strings.TrimSpace(val)
	if s == "" {
		return "unknown"
	}
	// Try int first.
	if _, err := strconv.Atoi(s); err == nil {
		return "int"
	}
	// Then try float.
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return "float"
	}
	// Then check for boolean.
	if strings.EqualFold(s, "true") || strings.EqualFold(s, "false") {
		return "bool"
	}
	return "string"
}

// inferJSONType returns a candidate type based on the Go type
// decoded from JSON. For numbers, it distinguishes between int and float.
func inferJSONType(val any) string {
	if val == nil {
		return "unknown"
	}
	switch v := val.(type) {
	case bool:
		return "bool"
	case float64:
		// Check if the float is an integer.
		if v == float64(int64(v)) {
			return "int"
		}
		return "float"
	case string:
		return "string"
	default:
		// For other types (objects, arrays), fallback to string.
		return "string"
	}
}

// combineTypes takes the current candidate type and a new detected type.
// If they differ (with a simple promotion rule), it returns a combined type.
// For example, int and float become float; if types conflict, we fallback to string.
func combineTypes(current, new string) string {
	if current == "unknown" {
		return new
	}
	if current == new {
		return current
	}
	// Allow int and float to combine into float.
	if (current == "int" && new == "float") || (current == "float" && new == "int") {
		return "float"
	}
	// If one is bool but the other is not, or any other conflict, fallback to string.
	return "string"
}

// parseDataTypeWithParameters parses data types that may contain length and precision parameters
// Examples: varchar(255), numeric(10,2), double(10,2)
// Returns: baseType, length, precision
func parseDataTypeWithParameters(dataType string) (string, int, int) {
	// Check if dataType contains parentheses
	if !strings.Contains(dataType, "(") || !strings.Contains(dataType, ")") {
		return dataType, 0, 0
	}

	// Split the dataType into base type and parameters
	parts := strings.SplitN(dataType, "(", 2)
	if len(parts) != 2 {
		return dataType, 0, 0
	}

	baseType := strings.TrimSpace(parts[0])
	paramsPart := strings.TrimSpace(parts[1])

	// Remove the closing parenthesis
	paramsPart = strings.TrimSuffix(paramsPart, ")")

	// Split parameters by comma
	params := strings.Split(paramsPart, ",")

	var length, precision int
	var err error

	// Parse first parameter (length)
	if len(params) > 0 {
		if length, err = strconv.Atoi(strings.TrimSpace(params[0])); err != nil {
			length = 0
		}
	}

	// Parse second parameter (precision) if it exists
	if len(params) > 1 {
		if precision, err = strconv.Atoi(strings.TrimSpace(params[1])); err != nil {
			precision = 0
		}
	}

	return baseType, length, precision
}
