package com.whylogs.api.logger;

import com.whylogs.core.DatasetProfile;
import com.whylogs.core.schemas.DatasetSchema;
import lombok.*;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Map;

@NoArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
public class TransientLogger extends Logger{
    public TransientLogger(DatasetSchema schema) {
        super(schema);
    }

    @Override
    protected ArrayList<DatasetProfile> getMatchingProfiles(Object data) {
        // In this case, we don't have any profiles to match against
        ArrayList<DatasetProfile> profiles = new ArrayList<>();
        DatasetProfile profile = new DatasetProfile(getSchema());
        profiles.add(profile);
        return profiles;
    }

    @Override
    protected <O> ArrayList<DatasetProfile> getMatchingProfiles(Map<String, O> data) {
        // In this case, we don't have any profiles to match against
        return getMatchingProfiles((Object) data);
    }
}
