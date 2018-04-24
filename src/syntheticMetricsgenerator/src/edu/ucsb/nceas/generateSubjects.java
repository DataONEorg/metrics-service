package edu.ucsb.nceas;

public class generateSubjects {

    public String[] Subject(String suffix, int len) {
        String[] repo = new String[len];

        for(int i = 0; i < len; i++) {
            repo[i] = suffix + String.format("%0"+((int)Math.log10(len)+1)+"d", (i+1));;
        }

        return repo;

    }
}
