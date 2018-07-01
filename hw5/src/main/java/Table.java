import java.util.HashSet;
import java.util.Set;


public class Table implements Resource{
    private String tableName;
    private Set<Page> pages;

    public Table (String tableName) {
        this.tableName = tableName;
        this.pages = new HashSet<Page>();
    }

    public String getTableName() {
        return this.tableName;
    }

    public Set<Page> getPages() {
        return this.pages;
    }

    public Page getPage(String pageName) {
        for (Page p : pages) {
            if (p.getName() == pageName && p.getTableName() == this.tableName) {
                return p;
            }
        }
        return null;
    }

    public void addPage(Page p) {
        pages.add(p);
    }

    public ResourceType getResourceType() {
        return ResourceType.TABLE;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof Table) {
            return ((Table) o).getTableName() == this.tableName;
        } else {
            return false;
        }
    }

}
