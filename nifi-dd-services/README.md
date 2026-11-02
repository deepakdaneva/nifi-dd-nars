# nifi-dd-services

Placeholder for the controller service **implementations** of nifi-dd-nars. It contains no services yet; it exists to
show the intended project structure for upcoming controller services.

- Packaging: `jar` (`io.github.deepakdaneva.nifi:nifi-dd-services`)
- Contents: implementations of the interfaces from `nifi-dd-services-api`, in the package
  `io.github.deepakdaneva.nifi.services`, registered in
  `src/main/resources/META-INF/services/org.apache.nifi.controller.ControllerService`
- Depends on: `nifi-api` and `nifi-dd-services-api` (both `provided`: they come from NiFi and the parent NAR)

## Place in the NAR hierarchy

```
nifi-standard-services-api-nar        (Apache NiFi)
  +-- nifi-dd-services-api-nar        interfaces (nifi-dd-services-api)
        +-- nifi-dd-services-nar      bundles this jar
```
