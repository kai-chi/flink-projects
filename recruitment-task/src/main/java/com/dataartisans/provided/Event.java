package com.dataartisans.provided;

/**
 * NOTE: Everything in the "provided" package IS NOT representative for
 * the level of quality we are expecting for the coding task submission.
 */
public class Event {
    public long time;
    public String someData;

    public Event() {
    }

    public Event(long time, String someData) {
        this.time = time;
        this.someData = someData;
    }

    @Override
    public String toString() {
        return "Event(" + time + ", " + someData + ')';
    }
}
