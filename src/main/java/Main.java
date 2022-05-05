import dao.impl.CategoriesServiceImpl;
import dao.services.CategoriesService;
import pojo.Categories;

public class Main {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);



    public static void main(String[] args) {
        CategoriesService categoryService = new CategoriesServiceImpl();
        util.Dataset<Categories> categoryDataset;
        categoryDataset = categoryService.getCategoriesList();
        categoryDataset.show();
    }

}