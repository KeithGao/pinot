package com.linkedin.thirdeye.bootstrap.aggregation;

/**
 * 
 *
 */
public enum AggregationJobConstants {
  
  AGG_INPUT_AVRO_SCHEMA("aggreation.input.avro.schema"), //
  AGG_INPUT_PATH("aggregation.input.path"), //
  AGG_OUTPUT_PATH("aggregation.output.path"), //
  AGG_CONFIG_PATH("agregation.config.path");//
  
  String name;

  AggregationJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
  
}
