import os
from luigi.contrib.spark import SparkSubmitTask
from luigi.contrib.hdfs import HdfsTarget
import luigi

os.environ["HADOOP_CONF_DIR"]="/etc/hadoop/conf"

user = "a.marchenko"

class PriceStat(SparkSubmitTask):
    
    app = "/home/{}/hadoop3/script3/price_stat.py".format(user)

    price_path = luigi.Parameter()
    product_for_stat_path = luigi.Parameter()
    output_path = luigi.Parameter() 

    def output(self):
        return HdfsTarget(self.output_path)
    
    def app_command(self):
        return [
            self.app,
            self.price_path,
            self.product_for_stat_path,
            self.output_path,
        ]

class OkDem(SparkSubmitTask):
    
    app = "/home/{}/hadoop3/script3/ok_dem.py".format(user)
    
    price_stat_path = luigi.Parameter()
    demography_path = luigi.Parameter()
    city_path = luigi.Parameter()
    city_rs_path = luigi.Parameter()
    price_path = luigi.Parameter()
    current_dt = "2023-03-01"
    output_path = luigi.Parameter()

    def output(self):
        return HdfsTarget(self.output_path)
    
    def requires(self):
        return PriceStat()
    
    def app_command(self):
        return [
            self.app,
            self.price_stat_path,
            self.demography_path,
            self.city_path,
            self.city_rs_path,
            self.price_path,
            self.current_dt,
            self.output_path
        ]

class ProductStat(SparkSubmitTask):
    
    app = "/home/{}/hadoop3/script3/product_stat.py".format(user)
    
    ok_dem_path = luigi.Parameter()
    city_path = luigi.Parameter()
    price_path = luigi.Parameter()
    product_path = luigi.Parameter()
    product_for_stat_path = luigi.Parameter()
    output_path = luigi.Parameter()
    
    def output(self):
        return HdfsTarget(self.output_path)
    
    def requires(self):
        return OkDem()
    
    def app_command(self):
        return [
            self.app,
            self.ok_dem_path,
            self.city_path,
            self.price_path,
            self.product_path,
            self.product_for_stat_path,
            self.output_path,
        ]
    
if __name__ == "__main__":
    luigi.run()
