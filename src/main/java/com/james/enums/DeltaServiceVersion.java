package com.james.enums;

public enum DeltaServiceVersion {

    V1("v1", Light.class, Driver.class);

    private final String version;
    private final Class<?>[] serviceClasses;

    private DeltaServiceVersion(final String v, final Class<?>... classes) {
        this.version = v;
        this.serviceClasses = classes;
    }

    public String getVersion() {
        return this.version;
    }

    public Class<?>[] getServiceClasses() {
        return this.serviceClasses;
    }

}
