### What is this repository for? ###

* this repo is an extractor for Xero API  (https://www.xero.com/)

### How do I get set up? ###

* Summary of set up

  pull this project down to your local repo

* Configuration

  * Project configuration is handled by ./config.py
  * Sensitive configuration values are read in from env vars, and necessitate the user to source in a bash script setting these values like so  

    set-env-vars.sh

  ```sh
    export CLIENT_ID=xxxxxxx
    export CLIENT_SECRET=yyyyyyyyyy
    export XERO_TENANT_ID=zzzzzzzzzz
  ```

* Dependencies

  * see requirements.txt
  * Python 3.6 

  
### Running the code! ###

 * to pull down target data entities (Contacts, Invoices, Items) execute the following 
    
  ```sh
  python main.py process_data    
  ```
 
 * to refresh access token
   
  ```sh
  python main.py refresh_token    
  ```
    
