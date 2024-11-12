# Tupi
 Analytics data pipeline

1. Run the docker compose command to create the environment
> docker-compose up -d --build

2. Run Kakfa producer
> python producer.py

3. Access Kafka Control Center
> http://localhost:9021/

4. To check generated JSONs access Topics > cc_retail_data > messages

5. Configure data sink, access Connect > connect-default > Add connector > Upload connector config file. Choose the file 'es-skink.properties' > Continue > Launch

6. Access Kibana
> http://localhost:5601/

7. Create a dashboard