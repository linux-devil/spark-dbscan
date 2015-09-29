package bean;

/**
 * Created by wlucia on 29/09/15.
 */

import java.io.Serializable;

/**
 *
 * @author wlucia
 */
public class GeoPoint implements Serializable {

    private Long id;
    private Double lat;
    private Double lng;


    public GeoPoint() {}

    public GeoPoint(Long id, Double lat, Double lng) {
        this.id = id;
        this.lat = lat;
        this.lng = lng;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLng() {
        return lng;
    }

    public void setLng(Double lng) {
        this.lng = lng;
    }


    public double[] getLatLng() {
        return new double[]{lat, lng};
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(id);
        sb.append(";");
        sb.append(lat);
        sb.append(";");
        sb.append(lng);
        sb.append(";");
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GeoPoint) {
            return this.equals((GeoPoint) o);
        } else {
            return false;
        }
    }

    public boolean equals(GeoPoint s) {
        return this.getId() == s.getId() &&
                this.getLat().equals(s.getLat())
                && this.getLng().equals(s.getLng());
    }

}
