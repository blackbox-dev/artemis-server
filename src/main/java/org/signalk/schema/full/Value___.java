
package org.signalk.schema.full;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "altitude",
    "latitude",
    "longitude"
})
public class Value___ {

    @JsonProperty("altitude")
    public Double altitude;
    @JsonProperty("latitude")
    public Double latitude;
    @JsonProperty("longitude")
    public Double longitude;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public Value___ withAltitude(Double altitude) {
        this.altitude = altitude;
        return this;
    }

    public Value___ withLatitude(Double latitude) {
        this.latitude = latitude;
        return this;
    }

    public Value___ withLongitude(Double longitude) {
        this.longitude = longitude;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public Value___ withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("altitude", altitude).append("latitude", latitude).append("longitude", longitude).append("additionalProperties", additionalProperties).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(altitude).append(additionalProperties).append(latitude).append(longitude).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Value___) == false) {
            return false;
        }
        Value___ rhs = ((Value___) other);
        return new EqualsBuilder().append(altitude, rhs.altitude).append(additionalProperties, rhs.additionalProperties).append(latitude, rhs.latitude).append(longitude, rhs.longitude).isEquals();
    }

}