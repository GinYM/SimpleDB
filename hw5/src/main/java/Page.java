
public class Page implements Resource {
  private String pageName;
  private Table table;

  public Page (String pageName, Table table) {
    this.pageName = pageName;
    this.table = table;
    this.table.addPage(this);
  }

  public String getName() {
    return this.pageName;
  }

  public Table getTable() {
    return this.table;
  }

  public String getTableName() {
    return this.table.getTableName();
  }

  public ResourceType getResourceType() {
    return ResourceType.PAGE;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    } else if (o instanceof Page) {
      return ((Page) o).getTable().equals(this.table) && ((Page) o).getName() == this.pageName;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format(
            "Page<table=%s, page=%s>",
            pageName, this.getTableName());
  }
}
