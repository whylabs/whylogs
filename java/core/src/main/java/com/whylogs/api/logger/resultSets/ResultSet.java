package com.whylogs.api.logger.resultSets;

/**
    A holder object for profiling results.

    A whylogs.log call can result in more than one profile. This wrapper class
    simplifies the navigation among these profiles.

    Note that currently we only hold one profile but we're planning to add other
    kinds of profiles such as segmented profiles here.
**/
public abstract class ResultSet {

    public static ResultSet read(String multiProfileFile){

    }

}
