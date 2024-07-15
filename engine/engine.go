package engine

import (
	"fmt"
	"strings"
)

func ParseQuery(query string) (*Query, error) {
	parts := strings.Fields(query)

	if len(parts) < 4 {
		return nil, fmt.Errorf("Invalid query format provided")
	}

	q := &Query{
		Select:  parts[1:3],
		From:    parts[3],
		Where:   "",
		OrderBy: "",
		Limit:   0,
	}

	if len(parts) > 4 {
		for i := 4; i < len(parts); i++ {
			switch strings.ToUpper(parts[i]) {
			case "WHERE":
				q.Where = parts[i+1]
				i++
			case "ORDER":
				if i+2 < len(parts) && strings.ToUpper(parts[i+1]) == "BY" {
					q.OrderBy = parts[i+2]
					i += 2
				}
			case "LIMIT":
				if i+1 < len(parts) {
					fmt.Sscan(parts[i+1], &q.Limit)
					i++
				}
			}
		}
	}

	return q, nil
}
