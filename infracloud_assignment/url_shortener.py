import string
import pandas as pd
import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()


class URL_Shortener:
    '''
    Reading from a csv file which contains previous original urls and 
    their corresponding ids
    '''
    logger.info('Starting Assignment')
    logger.info('Reading from existing csv file url_shorten.csv')
    df = pd.read_csv('url_shorten.csv')

#     extracting ids from shortened urls
    existing_id = [int(x[10:]) for x in df['short_url'].to_list()]
    existing_urls = dict(zip(df['URLs'].to_list() ,existing_id))
    if existing_id is not None and len(existing_id)!=0:
        id = existing_id[-1]+1
    else:
        id = 1

    def short_url(self, org_url):
        '''
        Check if the entered url is in list of urls previously present in csv file
        '''
        if org_url in self.existing_urls:
            logger.info('Url is already present in the csv file')
            id = self.existing_urls[org_url]
            short_url = self.encode_base(id)
        else:
#             Append id of new url
            logger.info('New url found, appending url and corresponding id to the dictionary')
            self.existing_urls[org_url] = self.id
            short_url = self.encode_base(self.id)
            # increase cnt for next url
            self.id += 1
        return ('shortened url is bitly.com/'+ short_url)
    
    def encode_base(self, id):
        # base 62 characters
        total_chars = string.digits+ string.ascii_letters
        base = len(total_chars)
        urls = []
        '''
        To encode according to base of all the alphanumeric characters. 
        It is able to give shortened urls for billions of urls 
        '''
        while id > 0:
            logger.info('Encoding to shorten large no of urls')
            val = id % base
            urls.append(total_chars[val])
            id = id // base
        # since urls has reversed order of base62 id, reverse urls before return it
        return "".join(urls[::-1])

def create_df(existing_urls):
    '''
    Empty the DataFrame and load all the values
    '''
    logger.info("Creating final Dataframe having old and new urls")
    df = pd.DataFrame()
    df['URLs'] = list(existing_urls.keys())
    df['short_url'] =["bitly.com/"+str(x) for x in list(existing_urls.values())]
    return df

def main():
    '''
    To create a Url Shortener from existing url as an input and save into a csv file
    '''
    shortener = URL_Shortener()
    val_url = input('Please enter url: ')
    print(shortener.short_url(val_url))

    # Write all the newly formed and old data- urls and their ids to csv 
    df = create_df(shortener.existing_urls)
    df.to_csv('url_shorten.csv' , index= False)
    logger.info('Saved all urls into the csv file')
    logger.info('Assignment completed')

if __name__ == "__main__":
    main()