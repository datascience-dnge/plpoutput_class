from pyspark.sql import SparkSession
from sys import argv, exit
import time
from pyspark.sql.functions import *
import pandas as pd  


class plpout:
	def  __init__(self,spark,base,case_std,date,ruta="bases",ord_hid=False,ver="GGNE",horario=False):

		self.base=base
		self.spark=spark
		self.case_std=case_std
		self.date=date
		self.ruta=ruta
		self.ord_hid=ord_hid
		self.path=base+"/"+date+"/"+case_std
		self.ver=ver
		self.horario=horario

	def clientes(self,filename): 
		ruta=self.ruta
		spark=self.spark

		#importar archivos de base
		clientes=spark.read.format("csv")\
					  .options(header=True,inferSchema=True)\
					  .load(ruta+'/'+filename+'.csv')

		#Limpieza de data de clientes
		clientes=clientes.withColumn("Fecha",(unix_timestamp("Fecha","dd-MM-yyyy")+(col("Hora")-1)*3600).cast("timestamp"))\
				 .selectExpr("Fecha as fecha","Retiros as fisico","cliente","barra_1 as barnom","contrato as descripcion")\
				 .withColumn("barnom",lower(rtrim(ltrim(col("barnom")))))

		return clientes		   

	def to_df(self,filename):
		path=self.path
		spark=self.spark
		df=spark.read.format("csv")\
			.options(header=True,inferSchema=True)\
			.load(path+"/"+filename +".csv")
		return df

	def etapas(self):
 		etapas_df=self.to_df("etapas")\
        		      .withColumnRenamed("#Bloque","Bloque")\
			      .withColumnRenamed(" Categoria","Categoria")\
			      .withColumn("Fecha",(unix_timestamp(" Fecha","dd-MM-yyyy").cast("timestamp")))\
			      .drop(" Fecha")
		return etapas_df

	def indhor(self):
		indhor_df=self.to_df("indhor")\
			      .withColumn("fecha", concat_ws("-", "Dia", "Mes", "Anio"))\
			      .withColumn("fecha",(unix_timestamp("fecha","dd-MM-yyyy")+(col("Hora")-1)*3600).cast("timestamp"))
		return indhor_df

	def ordhid(self):
		ordhid_df=self.to_df("ordhid")\
			      .withColumn("fecha", unix_timestamp("Fecha","dd-MM-yyyy").cast("timestamp"))\
			      .withColumn("Sim", col("Sim").cast('string'))\
			      .selectExpr("fecha as mes","Sim as hidro","Index")				
		return ordhid_df

    	def plpbar(self):
    			
    		plpbar_df=self.to_df("plpbar")
    		df_ordhid=self.ordhid()
    		etapas_df=self.etapas()
    		ord_hid=self.ord_hid
    		date=self.date
		case_std=self.case_std	
		plpbars=plpbar_df.join(etapas_df,["Bloque"],how="left_outer")\
				 .withColumnRenamed(" Duracion","Duracion")\
				 .selectExpr("cmgbar","Bloque","Hidro","barnom","Fecha","Categoria","Duracion")\
				 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
				 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
			         .withColumn("caso",lit(date+"_"+case_std))\
			         .withColumn("mes",date_trunc("month","fecha"))
		if ord_hid:			     
			plpbar_final=plpbars.join(self.df_ordhid,(["hidro","mes"])).drop("mes","hidro")
		else:
			plpbar_final=plpbars.withColumnRenamed("Hidro","Index").drop("mes")	
		return plpbar_final		  
	
	def plpcen(self):
    	
    		df_ordhid=self.ordhid()
    		ord_hid=self.ord_hid
    		etapas_df=self.etapas()
		plpcen_df=self.to_df("plpcen")
		date=self.date
		case_std=self.case_std
		plpcens=plpcen_df.join(etapas_df,["Bloque"])\
				 .withColumnRenamed(" Duracion","Duracion")\
			         .selectExpr("cenegen as fisico","Bloque","Hidro","cennom","ceninye as valorizado","fecha","barnom","cencvar","Categoria","Duracion")\
				 .withColumn("cennom",ltrim(rtrim(col("cennom"))))\
				 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
				 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
			         .withColumn("caso",lit(date+"_"+case_std))\
			         .withColumn("mes",date_trunc("month","fecha"))
		
		if ord_hid:			     
			plpcen_final=plpcens.join(df_ordhid,(["hidro","mes"])).drop("mes","hidro")
		else:
			plpcen_final=plpcens.withColumnRenamed("Hidro","Index").drop("mes")
		
		return plpcen_final		  



        def plpbar_hor(self):
                df_ordhid=self.ordhid()
                ord_hid=self.ord_hid
                indhor_df=self.indhor()
                plpbar_df=self.to_df("plpbar")
                date=self.date
                case_std=self.case_std

                plpbars=plpbar_df.join(indhor_df,["Bloque"])\
                                 .selectExpr("cmgbar","Hidro","fecha","barnom")\
                                 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
                                 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
                                 .withColumn("caso",lit(date+"_"+case_std))\
                                 .withColumn("mes",date_trunc("month","fecha"))

                if ord_hid:
                        plpbar_final=plpbars.join(df_ordhid,(["hidro","mes"])).drop("mes","hidro")
                else:
                        plpbar_final=plpbars.withColumnRenamed("Hidro","Index").drop("mes")
		
		return plpbar_final

	def plpcen_hor(self):
		df_ordhid=self.ordhid()
                ord_hid=self.ord_hid
                indhor_df=self.indhor()
                plpcen_df=self.to_df("plpcen")
                date=self.date
                case_std=self.case_std

                plpcens=plpcen_df.join(indhor_df,"Bloque")\
                                 .selectExpr("cenpgen as fisico","Hidro","cennom","ceninyp as valorizado","fecha","barnom","cencvar")\
                                 .withColumn("cennom",ltrim(rtrim(col("cennom"))))\
                                 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
                                 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
                                 .withColumn("caso",lit(date+"_"+case_std))\
                                 .withColumn("mes",date_trunc("month","fecha"))

                if ord_hid:
                        plpcen_final=plpcens.join(df_ordhid,(["hidro","mes"])).drop("mes","hidro")
                else:
                        plpcen_final=plpcens.withColumnRenamed("Hidro","Index").drop("mes")

                return plpcen_final

	def plp_bar_cen(self):
		
		if self.horario:
			plpbar_df=self.plpbar_hor()
			plpcen_df=self.plpcen_hor() 
		
		else:
			plpbar_df=self.plpbar().groupby("Index","barnom","fecha","categoria","caso")\
				       	       .agg((sum(col("duracion")*col("cmgbar"))/sum("duracion")).alias("CMg"))

			plpcen_df=self.plpcen().groupby("Index","barnom","fecha","categoria","caso","cennom")\
				       	       .agg(avg("cencvar").alias("cvar"),sum("fisico").alias("fisico"),sum("valorizado").alias("valorizado"))
		return plpbar_df,plpcen_df


	def centrales(self):
		ver=self.ver
		ruta=self.ruta
		spark=self.spark
		if ver=="GGNE":
			centrales_df=spark.read.format("csv")\
				       .options(header=True,inferSchema=True)\
				       .load(ruta+'/cennom_ggne.csv')\
				       .withColumn("cennom",ltrim(rtrim(col("cennom"))))
		else:
			centrales_df=spark.read.format("csv")\
				       .options(header=True,inferSchema=True)\
				       .load(ruta+'/cennom.csv')\
				       .withColumn("cennom",ltrim(rtrim(col("cennom"))))
		return centrales_df

	def iny_ret(self,retcsv):
		
		clientes_df=self.clientes(retcsv)
		plpbar_final=self.plpbar()
		plpcen_final=self.plpcen()
		centrales=self.centrales()
		indhor_df=self.indhor()
		etapas_df=self.etapas()
		date=self.date
		case_std=self.case_std

		cl=clientes_df.join(indhor_df,"fecha")\
			      .drop("fecha","Anio","Hora","Dia")\
		              .join(etapas_df,"Bloque")\
			      .groupby("Bloque","descripcion","cliente","barnom")\
			      .agg((sum("fisico")/1000).alias("fisico"))
		# Retiros Valorizados
		ret_val=plpbar_final.join(cl,["barnom","Bloque"]).withColumn("valorizado",-col("fisico")*col("cmgbar"))\
				    .selectExpr("fecha","descripcion","fisico","valorizado","Index","barnom as barra","Bloque","Categoria")\
				    .withColumn("tipo",lit("ret"))

		#Fecha limite
		max_date=ret_val.agg({"fecha": "max"}).collect()[0]["max(fecha)"]

		#Inyeccion valorizada    
		iny_val=plpcen_final.join(centrales,["cennom"])\
				    .selectExpr("fecha","Central as descripcion","fisico","valorizado","Index","barnom as barra","Bloque","Categoria")\
				    .withColumn("tipo",lit("iny"))

		#Filtro de fecha
		iny_val=iny_val.filter(iny_val.fecha<=max_date)

		#Balance Inyecciones y retiros
		balance=iny_val.unionAll(ret_val).withColumn("caso",lit(date+"_"+case_std))
	    
		return balance,plpbar_final,plpcen_final

	def saveToHive(self,df,table,db,hivemode):
		spark=self.spark
		spark.sql("create database if not exists "+db)

		if hivemode=='new':
			df.write.format("parquet").partitionBy("Index","caso").saveAsTable(db+"."+table)
		else:
			df.write.format("parquet").mode(hivemode).partitionBy("Index","caso").saveAsTable(db+"."+table)
