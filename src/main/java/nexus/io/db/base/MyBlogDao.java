package nexus.io.db.base;

public class MyBlogDao extends DbBase {

  @Override
  public String getTableName() {
    return "my_blog";
  }
}
