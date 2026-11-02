# nifi-dd-services-api

Placeholder for the controller service **API** of nifi-dd-nars. It contains no services yet; it exists to show the
intended project structure for upcoming controller services.

- Packaging: `jar` (`io.github.deepakdaneva.nifi:nifi-dd-services-api`)
- Contents: Java interfaces extending `org.apache.nifi.controller.ControllerService`, in the package
  `io.github.deepakdaneva.nifi.services`
- Depends on: `nifi-api` (provided by NiFi)

## Place in the NAR hierarchy

```
nifi-standard-services-api-nar        (Apache NiFi)
  +-- nifi-dd-services-api-nar        bundles this jar
        +-- nifi-dd-services-nar      implementations (nifi-dd-services)
```

The interfaces are loaded once, by `nifi-dd-services-api-nar`. Implementations and components that use the services
compile against this jar with `provided` scope and declare `nifi-dd-services-api-nar` as their parent NAR, so they
share the same interface classes at runtime.
