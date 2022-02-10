import com.whylogs.core.DatasetProfile;
import java.time.Instant;

public class SmokeTest {
    public static void main(String[] args) {
       DatasetProfile dsp = new DatasetProfile("test", Instant.now());
    }
}
