import os
import mysql.connector
from itemadapter import ItemAdapter


class TopCVPipeline:
    def open_spider(self, spider):
        self.conn = mysql.connector.connect(
            host=os.getenv("CRAWLER_DATABASE_HOST"),
            user=os.getenv("CRAWLER_DATABASE_USERNAME"),
            password=os.getenv("CRAWLER_DATABASE_PASSWORD"),
            database=os.getenv("CRAWLER_DATABASE_NAME")
        )
        self.cur = self.conn.cursor()


        self.conn.commit()

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Convert quantity to integer, handle potential conversion issues
        quantity = adapter.get('quantity', '')
        try:
            quantity = int(quantity) if quantity else None
        except ValueError:
            quantity = None

        self.cur.execute(
            """
            INSERT INTO jobs(
                job_url, 
                title,

                salary_range,
                location,
                
                description,
                requirements,
                benefit,
                
                company_url,
                company_name,
                company_avatar,
                company_scale,
                company_address,
                
                
                position,
                experience,
                
                quantity,
                
                type,
                gender,
                
                branch,
                
                updated_at,
                source
            ) 
            VALUES (
                %s, 
                %s,

                %s,
                %s,

                %s,
                %s,
                %s,

                %s,
                %s,
                %s,
                %s,
                %s,

                %s,

                %s,
                %s,

                %s,

                %s,

                %s,

                %s,
                %s
            )
            """,
            (
                adapter.get('job_url', ''),
                adapter.get('title', ''),

                adapter.get('salary_range', ''),
                adapter.get('location', ''),

                adapter.get('description', ''),
                adapter.get('requirements', ''),
                adapter.get('benefit', ''),

                adapter.get('company_url', ''),
                adapter.get('company_name', ''),
                adapter.get('company_avatar', ''),
                adapter.get('company_scale', ''),
                adapter.get('company_address', ''),

                adapter.get('position', ''),
                adapter.get('experience', ''),

                quantity,

                adapter.get('type', ''),
                adapter.get('gender', ''),

                adapter.get('branch', ''),

                adapter.get('updated_at', ''),
                adapter.get('source', '')
            )
        )

        self.conn.commit()

        return item
