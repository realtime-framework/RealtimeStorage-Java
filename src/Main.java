import co.realtime.storage.ItemSnapshot;
import co.realtime.storage.StorageRef;
import co.realtime.storage.TableRef;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.StorageException;

public class Main {

    public static void main(String[] args) {

        try {
            StorageRef storage = new StorageRef("YOUR_APPLICATION_KEY", "YOUR_PRIVATE_KEY","YOUR_TOKEN", true, false,"https://storage-balancer.realtime.co/server/ssl/1.0");
            TableRef tableRef = storage.table("todoTable");

            // Retrieve all the items in SomeTable

            tableRef.getItems(new OnItemSnapshot(){
                                  public void run(ItemSnapshot itemSnapshot) {
                                      if(itemSnapshot != null) {
                                          System.out.println(String.format("Item retrieved: %s", itemSnapshot.val()));
                                      } else {
                                          System.out.println("No more items in table");
                                      }
                                  }
                              },
                    new OnError(){
                        public void run(Integer code, String errorMessage) {
                            System.out.println(String.format("Oops, error retrieving items: %s", errorMessage));
                        }
                    });

            // Be notified in realtime when an item is updated in SomeTable

            tableRef.on(StorageRef.StorageEvent.UPDATE, new OnItemSnapshot(){
                public void run(ItemSnapshot itemSnapshot) {
                    System.out.println(String.format("Item updated: %s", itemSnapshot.val()));
                }
            });

        } catch (StorageException e) {
            e.printStackTrace();
        }
    }
}
