package indexing;

/**
 * Created by wlucia on 29/09/15.
 */
public class CloudSearchServiceUtils {
    private static final double EARTH_RADIUS_IN_METERS = 6378388d;

    public static double getHaversineDistance(double lat2, double lat1, double lon2, double lon1) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double dist = EARTH_RADIUS_IN_METERS * c;

        return dist;
    }
}
