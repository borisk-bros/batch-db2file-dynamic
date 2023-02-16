package com.example.batchprocessing;

import com.example.batchprocessing.model.Person;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private SqlSessionFactory sqlSessionFactory;

	@Bean
	public SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception {
		SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
		factoryBean.setDataSource(dataSource);
		return factoryBean.getObject();
	}

	@Bean
	public MyBatisCursorItemReader<Person> myBatisReader() {
		final MyBatisCursorItemReader<Person> reader = new MyBatisCursorItemReader<>();
		reader.setSqlSessionFactory(sqlSessionFactory);
		reader.setQueryId("com.example.batchprocessing.repository.PersonRepository.findByAge");
		reader.setParameterValues(Stream.of(
						new AbstractMap.SimpleImmutableEntry<>("ageFrom", 1),
						new AbstractMap.SimpleImmutableEntry<>("ageTo", 10))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
		return reader;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public FlatFileItemWriter<Person> writer(){
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("output.csv"));
		writer.setLineAggregator(new DelimitedLineAggregator<Person>() {{
			setDelimiter(",");
			setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {{
				setNames(new String[] { "firstName", "lastName" });
			}});
		}});

		return writer;
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(step1)
				.end()
				.build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.<Person, Person> chunk(10)
				.reader(myBatisReader())
				.processor(processor())
				.writer(writer())
				.build();
	}
}
