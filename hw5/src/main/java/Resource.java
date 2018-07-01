public interface Resource {

  public enum ResourceType {
    TABLE,
    PAGE
  }

  public ResourceType getResourceType();

  public String getTableName();

  @Override
  public boolean equals(Object o);

}
