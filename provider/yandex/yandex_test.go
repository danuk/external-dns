/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package yandex

import (
	"testing"

    dns "github.com/yandex-cloud/go-genproto/yandex/cloud/dns/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

    "sigs.k8s.io/external-dns/endpoint"
    "sigs.k8s.io/external-dns/plan"
)

func TestApplyChanges (t *testing.T) {

	createRecords := []*endpoint.Endpoint{
		endpoint.NewEndpoint("create-test.zone-1.ext-dns-test-2.yandex.ru.", endpoint.RecordTypeA, "8.8.8.8"),
		endpoint.NewEndpointWithTTL("create-test-ttl.zone-2.ext-dns-test-2.yandex.ru", endpoint.RecordTypeA, endpoint.TTL(15), "8.8.4.4"),
		endpoint.NewEndpoint("nomatch-create-test.zone-0.ext-dns-test-2.domain.com", endpoint.RecordTypeA, "4.2.2.1"),
	}

	currentRecords := []*endpoint.Endpoint{}

	updatedRecords := []*endpoint.Endpoint{
	    endpoint.NewEndpoint("create-test-cname.zone-1.ext-dns-test-2.yandex.ru", endpoint.RecordTypeCNAME, "foo.elb.amazonaws.com"),
	}

	deleteRecords := []*endpoint.Endpoint{
		endpoint.NewEndpoint("delete-test.zone-1.ext-dns-test-2.yandex.ru", endpoint.RecordTypeA, "8.8.8.8"),
	}

	changes := &plan.Changes{
		Create:    createRecords,
		UpdateNew: updatedRecords,
		UpdateOld: currentRecords,
		Delete:    deleteRecords,
	}

    change := &dns.UpsertRecordSetsRequest{}
    change.Replacements = append(change.Replacements, newFilteredRecords(changes.Create)...)
    change.Replacements = append(change.Replacements, newFilteredRecords(changes.UpdateNew)...)
    change.Replacements = append(change.Replacements, newFilteredRecords(changes.UpdateOld)...)
    change.Deletions = append(change.Deletions, newFilteredRecords(changes.Delete)...)

    zones := []*dns.DnsZone{
        &dns.DnsZone{ Id: "xxxyyyzzz1", Zone: "danuk.ru.", Name: "danuk-ru", },
        &dns.DnsZone{ Id: "xxxyyyzzz2", Zone: "yandex.ru.", Name: "yandex.ru", },
        &dns.DnsZone{ Id: "xxxyyyzzz3", Zone: "domain.com.", Name: "domain-com", },
    }

    changesByZone := changesByZone(zones, change)

	assert.Equal(t, changesByZone["yandex.ru."].DnsZoneId, "xxxyyyzzz2")

	validateChange(t, changesByZone["yandex.ru."], &dns.UpsertRecordSetsRequest{
		Replacements: []*dns.RecordSet{
            {Name: "create-test.zone-1.ext-dns-test-2.yandex.ru.", Type:"A", Ttl: 300, Data: []string{"8.8.8.8"}},
            {Name: "create-test-ttl.zone-2.ext-dns-test-2.yandex.ru.", Type:"A", Ttl: 15, Data: []string{"8.8.4.4"}},
            {Name: "create-test-cname.zone-1.ext-dns-test-2.yandex.ru.", Type:"CNAME", Ttl: 300, Data: []string{"foo.elb.amazonaws.com."}},
		},
		Deletions: []*dns.RecordSet{
            {Name: "delete-test.zone-1.ext-dns-test-2.yandex.ru.", Type: "A", Ttl: 300, Data: []string{"8.8.8.8"} },
		},
	})

}

func validateChange(t *testing.T, change *dns.UpsertRecordSetsRequest, expected *dns.UpsertRecordSetsRequest) {
	validateChangeRecords(t, change.Replacements, expected.Replacements)
	validateChangeRecords(t, change.Deletions, expected.Deletions)
}

func validateChangeRecords(t *testing.T, records []*dns.RecordSet, expected []*dns.RecordSet) {
    require.Len(t, records, len(expected))

    for i := range records {
		validateChangeRecord(t, records[i], expected[i])
    }
}

func validateChangeRecord(t *testing.T, record *dns.RecordSet, expected *dns.RecordSet) {
	assert.Equal(t, expected.Name, record.Name)
	assert.Equal(t, expected.Data, record.Data)
	assert.Equal(t, expected.Ttl, record.Ttl)
	assert.Equal(t, expected.Type, record.Type)
}

