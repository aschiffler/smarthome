package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
)

func ReadUserIP(r *http.Request) string {
	IPAddress := r.Header.Get("X-Real-Ip")
	if IPAddress == "" {
		IPAddress = r.Header.Get("X-Forwarded-For")
	}
	if IPAddress == "" {
		IPAddress = r.RemoteAddr
	}
	return IPAddress
}

func main() {
	http.HandleFunc("/", handler)
	http.ListenAndServe("127.0.0.1:4000", nil)
}

func handler(w http.ResponseWriter, r *http.Request) {
	ip := r.FormValue("ip")
	hostname := r.FormValue("hostname")
	if ip == "" {
		ip = ReadUserIP(r)
	}
	if !validateIP(ip) {
		fmt.Fprintf(w, "Your IP Address %s", ip)
		w.WriteHeader(http.StatusBadRequest)
	} else {
		fmt.Fprintf(w, "Valid IP address %s", ip)
		zone := os.Getenv("ZONE")
		if zone != "" && hostname != "" {
			updateDNS(ip, hostname, zone)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, ", set for record %s", hostname)
			return
		}
		fmt.Fprintf(w, ", but no record updated")
	}
}

func validateIP(ip string) bool {

	// split the ip into its 4 octets
	octets := strings.Split(ip, ".")
	if len(octets) != 4 {
		return false
	}
	return true
}

func updateDNS(ip string, domain string, zone string) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("eu-central-1"),
		Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_KEY_ID"), os.Getenv("AWS_KEY_SECRET"), ""),
	})
	if err != nil {
		fmt.Println("Error creating session:", err)
		return
	}

	svc := route53.New(sess)

	input := &route53.ChangeResourceRecordSetsInput{
		ChangeBatch: &route53.ChangeBatch{
			Changes: []*route53.Change{
				{
					Action: aws.String(route53.ChangeActionUpsert),
					ResourceRecordSet: &route53.ResourceRecordSet{
						Name: aws.String(domain),
						Type: aws.String(route53.RRTypeA),
						TTL:  aws.Int64(300),
						ResourceRecords: []*route53.ResourceRecord{
							{
								Value: aws.String(ip),
							},
						},
					},
				},
			},
		},
		HostedZoneId: aws.String(zone),
	}

	_, err = svc.ChangeResourceRecordSets(input)
	if err != nil {
		fmt.Println("Error updating record set:", err)
		os.Exit(1)
	}

	fmt.Println("Record set updated successfully to ", ip, " for domain ", domain)
}
