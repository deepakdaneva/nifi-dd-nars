# nifi-dd-services-nar

Placeholder NAR for the controller service **implementations** of nifi-dd-nars. It contains no services yet; it exists
to show the intended project structure for upcoming controller services.

- Packaging: `nar` (`io.github.deepakdaneva.nifi:nifi-dd-services-nar`)
- Bundles: `nifi-dd-services`
- Parent NAR: `io.github.deepakdaneva.nifi:nifi-dd-services-api-nar`

## Place in the NAR hierarchy

```
nifi-standard-services-api-nar        (Apache NiFi)
  +-- nifi-dd-services-api-nar        interfaces (nifi-dd-services-api)
        +-- nifi-dd-services-nar      this NAR
```

Because the interfaces live in the parent NAR, this NAR bundles only the implementations, and other NARs can use the
services by declaring `nifi-dd-services-api-nar` as their parent.
