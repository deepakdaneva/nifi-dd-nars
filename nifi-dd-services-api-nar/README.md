# nifi-dd-services-api-nar

Placeholder NAR for the controller service **API** of nifi-dd-nars. It contains no services yet; it exists to show the
intended project structure for upcoming controller services.

- Packaging: `nar` (`io.github.deepakdaneva.nifi:nifi-dd-services-api-nar`)
- Bundles: `nifi-dd-services-api`
- Parent NAR: `org.apache.nifi:nifi-standard-services-api-nar`

## Place in the NAR hierarchy

```
nifi-standard-services-api-nar        (Apache NiFi)
  +-- nifi-dd-services-api-nar        this NAR
        +-- nifi-dd-services-nar      implementations (nifi-dd-services)
```

Any NAR that implements or uses the nifi-dd controller service interfaces declares this NAR as its parent, so all of
them share one copy of the interface classes.
