package br.com.fences.ocorrenciaespelhobatch.ocorrenciaespelho;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.batch.api.partition.PartitionPlan;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import br.com.fences.fencesutils.formatar.FormatarData;
import br.com.fences.fencesutils.rest.tratamentoerro.util.VerificarErro;
import br.com.fences.fencesutils.verificador.Verificador;
import br.com.fences.ocorrenciaespelhobatch.config.AppConfig;

@ApplicationScoped
public class OcorrenciaEspelhoPartitionPlan implements PartitionPlan{

	@Inject
	private transient Logger logger;
	
	@Inject
	private AppConfig appConfig;
	
	@Inject
	private VerificarErro verificarErro;
	
	
	private String dataInicialGeral;
	private String dataFinalGeral;
	
	@Override
	public void setPartitions(int count) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPartitionsOverride(boolean override) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean getPartitionsOverride() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setThreads(int count) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPartitionProperties(Properties[] props) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Quantidade de blocos que ira ser executado.
	 */
	@Override
	public int getPartitions() {		
		int qtd = getPartitionProperties().length;
		return qtd; 
	}

	@Override
	public int getThreads() {
		int qtd = 1;
		if (Verificador.isValorado(appConfig.getBatchThreads()))
		{
			qtd = Integer.parseInt(appConfig.getBatchThreads());
		}
		return qtd;
	}

	public void test()
	{
		getPartitionProperties();
	}

	
	@Override
	public Properties[] getPartitionProperties() {
		
		//String dataInicialGeral = "20100101000000";
		if (!Verificador.isValorado(dataInicialGeral))
		{
			dataInicialGeral = consultarDataInicial();
			logger.info("dataInicialGeral retornada[" + dataInicialGeral + "]");
		}
		Date dtInicialGeral = null;
		Calendar clInicialGeral = Calendar.getInstance();
		long longInicialGeral = 0;
		long longInicialLoop = 0;
		
		if (!Verificador.isValorado(dataFinalGeral))
		{
			dataFinalGeral = consultarDataFinal();
			logger.info("dataFinalGeral retornada[" + dataFinalGeral + "]");
		}
		Date dtFinalGeral = null;
		Calendar clFinalGeral = Calendar.getInstance();
		long longFinalGeral = 0;
		long longFinalLoop = 0;
		
		
		try {
			dtInicialGeral = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().parse(dataInicialGeral);
			dtFinalGeral = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().parse(dataFinalGeral);
		} catch (ParseException e) {
			throw new RuntimeException("Erro no parse de data.", e);
		}
		
		clInicialGeral.setTime(dtInicialGeral);
		clFinalGeral.setTime(dtFinalGeral);
		
		longInicialGeral = dtInicialGeral.getTime();
		longInicialLoop = longInicialGeral;
		longFinalGeral = dtFinalGeral.getTime();
		
		int bloco = 0;
		if (clInicialGeral.get(Calendar.YEAR) != clFinalGeral.get(Calendar.YEAR))
		{
			bloco = 1000 * 60 * 60 * 24 * 10; //-- 10dias
		}
		else if (clInicialGeral.get(Calendar.MONTH) != clFinalGeral.get(Calendar.MONTH))
		{
			bloco = 1000 * 60 * 60 * 24 * 5; //-- 5dias
		}
		else if (clInicialGeral.get(Calendar.DAY_OF_MONTH) != clFinalGeral.get(Calendar.DAY_OF_MONTH))
		{
			bloco = 1000 * 60 * 60 * 24; //-- 1dia
		}
		else
		{
			bloco = 1000 * 60 * 5; //-- 5minutos
		}
		longFinalLoop = longInicialGeral + bloco - 1;
		
		List<Properties> props = new ArrayList<>();
		while (longInicialLoop < longFinalGeral)
		{
			if (longFinalLoop > longFinalGeral)
			{
				longFinalLoop = longFinalGeral;
			}
			
			String inicio = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().format(new Date(longInicialLoop));
			String termino = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().format(new Date(longFinalLoop));
			
			//logger.debug("bloco inicio[" + inicio + "] termino[" + termino + "]");
			criarComAdicionarPropriedade(props, inicio, termino);
			
			longInicialLoop += bloco;
			longFinalLoop += bloco;
		}
		
		return props.toArray(new Properties[0]);
	}
	
	private String consultarDataInicial()
	{
		logger.info("Recuperar data inicial para PROCESSAR e REPROCESSAR do controle...");
		Client client = ClientBuilder.newClient();
		String servico = "http://" + appConfig.getServerBackendHost() + ":"+ appConfig.getServerBackendPort() + "/ocorrenciaespelhobackend/rest/" + 
				"espelhoOcorrenciaControle/pesquisarProcessarReprocessarDataInicial";
		WebTarget webTarget = client
				.target(servico);
		Response response = webTarget
				.request(MediaType.APPLICATION_JSON)
				.get();
		String json = response.readEntity(String.class);
		if (verificarErro.contemErro(response, json))
		{
			String msg = verificarErro.criarMensagem(response, json, servico);
			logger.error(msg); 
			throw new RuntimeException(msg);
		}
		return json;
	}
	
	private String consultarDataFinal()
	{
		logger.info("Recuperar data final para PROCESSAR e REPROCESSAR do controle...");
		Client client = ClientBuilder.newClient();
		String servico = "http://" + appConfig.getServerBackendHost() + ":"+ appConfig.getServerBackendPort() + "/ocorrenciaespelhobackend/rest/" + 
				"espelhoOcorrenciaControle/pesquisarProcessarReprocessarDataFinal";
		WebTarget webTarget = client
				.target(servico);
		Response response = webTarget
				.request(MediaType.APPLICATION_JSON)
				.get();
		String json = response.readEntity(String.class);
		if (verificarErro.contemErro(response, json))
		{
			String msg = verificarErro.criarMensagem(response, json, servico);
			logger.error(msg); 
			throw new RuntimeException(msg);
		}
		return json;
	}
	
	private Properties criarPropriedade(String inicio, String termino)
	{
		Properties prop = new Properties();	
		prop.setProperty("inicio", inicio);
		prop.setProperty("fim", termino);
		return prop;
	}
	
	private void criarComAdicionarPropriedade(List<Properties> propriedades, String inicio, String termino)
	{
		Properties prop = criarPropriedade(inicio, termino);
		propriedades.add(prop);
	}

}
