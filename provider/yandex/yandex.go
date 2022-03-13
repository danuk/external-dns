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
    "context"

    log "github.com/sirupsen/logrus"
    dns "github.com/yandex-cloud/go-genproto/yandex/cloud/dns/v1"

    ycsdk "github.com/yandex-cloud/go-sdk"

    "sigs.k8s.io/external-dns/endpoint"
    "sigs.k8s.io/external-dns/plan"
    "sigs.k8s.io/external-dns/provider"
)

const (
    yandexRecordTTL = 300
)

type YandexProvider struct {
    provider.BaseProvider
    ctx context.Context
    token string
    folderID string
    domainFilter endpoint.DomainFilter
    dryRun bool
    sdk ycsdk.SDK
}

func NewYandexProvider(ctx context.Context, token string, folder_id string, domainFilter endpoint.DomainFilter, dryRun bool) (*YandexProvider, error) {
    if len(token) == 0 {
        log.Fatal("yandex-token required")
        return nil, nil
    }

    if len(folder_id) == 0 {
        log.Fatal("yandex-folder-id required")
        return nil, nil
    }

    sdk, err := ycsdk.Build(ctx, ycsdk.Config{
        Credentials: ycsdk.OAuthToken(token),
    })
    if err != nil {
        return nil, err
    }

    provider := &YandexProvider{
        ctx:                      ctx,
        token:                    token,
        folderID:                 folder_id,
        domainFilter:             domainFilter,
        dryRun:                   dryRun,
        sdk:                      *sdk,
    }
    return provider, nil
}

func (p *YandexProvider) FetchZones(ctx context.Context, page_token string) (zones []*dns.DnsZone) {
    request := &dns.ListDnsZonesRequest{
        FolderId: p.folderID,
        //PageSize: 100,
        PageToken: page_token,
    }

    response, err := p.sdk.DNS().DnsZone().List( p.ctx, request )
    if err != nil {
        log.Fatal(err)
    }

    for _, item := range response.DnsZones {
        if p.domainFilter.Match(item.Zone) {
            zones = append(zones, item)
            log.Debugf("Matched %s (zone: %s)", item.Zone, item.Name)
        } else {
            log.Debugf("Filtered %s (zone: %s)", item.Zone, item.Name)
        }
    }

    for _, item := range zones {
        log.Debugf("Considering zone: %s (domain: %s)", item.Name, item.Zone)
    }

    if len(response.NextPageToken)>0 {
        log.Debugf("Fetch next token: %s", response.NextPageToken)
        zones = append(zones, p.FetchZones(p.ctx, response.NextPageToken)...)
    }

    return zones
}

func (p *YandexProvider) Records(ctx context.Context) (endpoints []*endpoint.Endpoint, _ error) {
    zones := p.FetchZones(ctx, "")

    var recordsByZone []*endpoint.Endpoint
    for _, zone := range zones {
        recordsByZone = p.FetchRecordsByZone( ctx, zone.Id, "" )
        endpoints = append( endpoints, recordsByZone...)
        log.Debugf("Records found in zone: %s, count records: %d", zone.Zone, len(recordsByZone) )
    }

    return endpoints, nil
}

func (p *YandexProvider) FetchRecordsByZone(ctx context.Context, zone string, page_token string) (endpoints []*endpoint.Endpoint ) {
    request := &dns.ListDnsZoneRecordSetsRequest{
        DnsZoneId: zone,
        //PageSize: 100,
        PageToken: page_token,
    }

    response, err := p.sdk.DNS().DnsZone().ListRecordSets( p.ctx, request )
    if err != nil {
        log.Warnf("No records found in zone: %s", zone)
        return nil
    }

    for _, r := range response.RecordSets {
        if !provider.SupportedRecordType(r.Type) {
            continue
        }
        log.Debugf("Fetch record: %s %s", r.Type, r.Name)
        endpoints = append(endpoints, endpoint.NewEndpointWithTTL(r.Name, r.Type, endpoint.TTL(r.Ttl), r.Data...))
    }

    if len(response.NextPageToken)>0 {
        log.Debugf("Fetch next token: %s", response.NextPageToken)
        endpoints = append(endpoints, p.FetchRecordsByZone(ctx, zone, response.NextPageToken)...)
    }

    return endpoints
}

func (p *YandexProvider) ApplyChanges(ctx context.Context, changes *plan.Changes) error {
    change := &dns.UpsertRecordSetsRequest{}

    change.Replacements = append(change.Replacements, newFilteredRecords(changes.Create)...)
    change.Replacements = append(change.Replacements, newFilteredRecords(changes.UpdateNew)...)
    change.Replacements = append(change.Replacements, newFilteredRecords(changes.UpdateOld)...)
    change.Deletions = append(change.Deletions, newFilteredRecords(changes.Delete)...)

    return p.submitChange(ctx, change)
}

func (p *YandexProvider) submitChange(ctx context.Context, change *dns.UpsertRecordSetsRequest) error {
    if len(change.Replacements) == 0 && len(change.Deletions) == 0 {
        log.Info("All records are already up to date")
        return nil
    }

    zones := p.FetchZones(ctx, "")

    changesByZone := changesByZone(zones, change)

    for zone, request := range changesByZone {
        log.Infof("UPDATE ZONE: %s", zone )

        for _, v := range request.Replacements {
            log.Infof("UPDATE: %v", v)
        }

        for _, v := range request.Deletions{
            log.Infof("DELETE: %v", v)
        }

        if p.dryRun {
            continue
        }
         _, err := p.sdk.DNS().DnsZone().UpsertRecordSets( ctx, request )
        if err != nil {
            return err
        }
    }

    return nil
}

func changesByZone(zones []*dns.DnsZone, change *dns.UpsertRecordSetsRequest) map[string]*dns.UpsertRecordSetsRequest {
    changes := make(map[string]*dns.UpsertRecordSetsRequest)
    zoneNameIDMapper := provider.ZoneIDName{}
    for _, z := range zones {
        zoneNameIDMapper.Add(z.Zone, z.Zone)
        changes[z.Zone] = &dns.UpsertRecordSetsRequest{
            Replacements: []*dns.RecordSet{},
            Deletions: []*dns.RecordSet{},
        }
        changes[z.Zone].DnsZoneId = z.Id
    }

    for _, a := range change.Replacements {
        if zoneName, _ := zoneNameIDMapper.FindZone(provider.EnsureTrailingDot(a.Name)); zoneName != "" {
            changes[zoneName].Replacements = append(changes[zoneName].Replacements, a)
        } else {
            log.Warnf("No matching zone for record addition: %s %s %s %d", a.Name, a.Type, a.Data, a.Ttl)
        }
    }

    for _, d := range change.Deletions {
        if zoneName, _ := zoneNameIDMapper.FindZone(provider.EnsureTrailingDot(d.Name)); zoneName != "" {
            changes[zoneName].Deletions = append(changes[zoneName].Deletions, d)
        } else {
            log.Warnf("No matching zone for record deletion: %s %s %s %d", d.Name, d.Type, d.Data, d.Ttl)
        }
    }
    return changes
}

func newFilteredRecords(endpoints []*endpoint.Endpoint) []*dns.RecordSet {
    records := []*dns.RecordSet{}

    for _, endpoint := range endpoints {
        records = append(records, newRecord(endpoint))
    }

    return records
}

func newRecord(ep *endpoint.Endpoint) *dns.RecordSet {
    targets := make([]string, len(ep.Targets))
    copy(targets, []string(ep.Targets))
    if ep.RecordType == endpoint.RecordTypeCNAME {
        targets[0] = provider.EnsureTrailingDot(targets[0])
    }

    var ttl int64 = yandexRecordTTL
    if ep.RecordTTL.IsConfigured() {
        ttl = int64(ep.RecordTTL)
    }

    return &dns.RecordSet{
        Name: provider.EnsureTrailingDot(ep.DNSName),
        Type: ep.RecordType,
        Ttl: ttl,
        Data: targets,
    }
}

