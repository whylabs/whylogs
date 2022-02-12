import com.whylogs.core.DatasetProfile;
import com.whylogs.core.statistics.NumberTracker;
import java.time.Instant;

public class SmokeTest {
    public static void main(String[] args) {
       DatasetProfile dsp = new DatasetProfile("test", Instant.now());
       NumberTracker tracker = new NumberTracker();
       val protobuf = tracker.toProtobuf();
    }
}
