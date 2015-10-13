package br.com.fences.ocorrenciaespelhobatch.ocorrenciaespelhocontrole;

import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.batch.api.AbstractBatchlet;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import br.com.fences.fencesutils.conversor.converter.Converter;
import br.com.fences.fencesutils.formatar.FormatarData;
import br.com.fences.fencesutils.rest.tratamentoerro.util.VerificarErro;
import br.com.fences.fencesutils.verificador.Verificador;
import br.com.fences.ocorrenciaentidade.chave.OcorrenciaChave;
import br.com.fences.ocorrenciaentidade.controle.ControleOcorrencia;
import br.com.fences.ocorrenciaespelhobatch.config.AppConfig;

@Named
public class OcorrenciaEspelhoControleBatchlet extends AbstractBatchlet {

	@Inject 
	private transient Logger logger;
	
	@Inject
	private AppConfig appConfig;
	
	@Inject
	private VerificarErro verificarErro;
	
	@Inject
	private Converter<OcorrenciaChave> converterOcorrenciaChave;

	@Inject
	private Converter<ControleOcorrencia> converterControleOcorrencia;

	private String host;
	private String port;

	
	private Set<OcorrenciaChave> ocorrenciasChaves = new LinkedHashSet<>();

	
	public String process() throws Exception {

		host = appConfig.getServerBackendHost();
		port = appConfig.getServerBackendPort();

		
		//------------- seleciona ultima data de registro
		logger.info("Recuperar ultima data de registro carregada...");
		Client client = ClientBuilder.newClient();
		String servico = "http://" + host + ":"+ port + "/ocorrenciaespelhobackend/rest/" + 
				"espelhoOcorrenciaControle/pesquisarUltimaDataRegistroNaoComplementar";
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
		String ultimaDataDeRegistro = json;  

		if (!Verificador.isValorado(ultimaDataDeRegistro))
		{
			Calendar anosAtras = Calendar.getInstance();
			anosAtras.add(Calendar.YEAR, -5);
			anosAtras.set(Calendar.HOUR_OF_DAY, 0);
			anosAtras.set(Calendar.MINUTE, 0);
			anosAtras.set(Calendar.SECOND, 0);
			anosAtras.set(Calendar.MILLISECOND, 0);
			
			ultimaDataDeRegistro = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().format(anosAtras.getTime());
			//ultimaDataDeRegistro = "20100801000000";
		}
		
		
		logger.info("Ultima data de registro carregada: " + ultimaDataDeRegistro);
		logger.info("Montando periodo de pesquisa...");

		Date dtUltimaDataDeRegistro = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().parse(ultimaDataDeRegistro);
		Calendar calUltimaDataDeRegistro = Calendar.getInstance();
		calUltimaDataDeRegistro.setTime(dtUltimaDataDeRegistro);
		calUltimaDataDeRegistro.add(Calendar.SECOND, 1); //-- para nao selecionar o ultimo registro carregado.
		String dataRegistroInicial = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().format(calUltimaDataDeRegistro.getTime());
		logger.info("Ultima data de registro adicionado 1 segundo a mais: " + dataRegistroInicial);
		
		Calendar calDataCorrente = Calendar.getInstance();
		calDataCorrente.add(Calendar.HOUR_OF_DAY, -1); //-- ajuste de seguranca
		String dataCorrente = FormatarData
				.getAnoMesDiaHoraMinutoSegundoConcatenados().format(
						calDataCorrente.getTime());
		
		String dataRegistroFinal = dataCorrente;

		
		//--------------------- seleciona chaves
		//TODO force2...
		//dataRegistroFinal = "20101111103409";
		
		logger.info("Periodo de pesquisa inicial[" + dataRegistroInicial + "] final[" + dataRegistroFinal + "]");
		logger.info("Loop por mes...");

		Date loopDataRegistroFinal = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().parse(dataRegistroFinal);
		Date loopDataRegistroInicial = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().parse(dataRegistroInicial);
		
		while (loopDataRegistroInicial.before(loopDataRegistroFinal))
		{
			Calendar dataRegistroSemifinal = Calendar.getInstance();
			dataRegistroSemifinal.setTime(loopDataRegistroInicial);
			dataRegistroSemifinal.add(Calendar.DAY_OF_WEEK, 10);	//-- periodicidade do loop
			//dataRegistroSemifinal.add(Calendar.HOUR_OF_DAY, 1);	//-- periodicidade do loop
			if (dataRegistroSemifinal.after(loopDataRegistroFinal))
			{
				dataRegistroSemifinal.setTime(loopDataRegistroFinal);
			}
			
			Date loopDataRegistroSemifinal = dataRegistroSemifinal.getTime();
			
			
			String loopDataRegistroInicialString = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().format(loopDataRegistroInicial);
			String loopDataRegistroInicialSemifinalString = FormatarData.getAnoMesDiaHoraMinutoSegundoConcatenados().format(loopDataRegistroSemifinal);
			
			logger.info("Pesquisa no loop inicial[" + loopDataRegistroInicialString + "] final[" + loopDataRegistroInicialSemifinalString + "]...");
			
			
			//----
			ocorrenciasChaves = listarChaves(loopDataRegistroInicialString, loopDataRegistroInicialSemifinalString);
			logger.info("Foram lidas [" + ocorrenciasChaves.size() + "] chaves para carga.");

		
			//-- itera chaves
			int count = 0;
			for (OcorrenciaChave ocorrenciaChave : ocorrenciasChaves)
			{
				count++;
				
				registrarChave(ocorrenciaChave);
				
				if ( (count % 5) == 0 )
				{
					logger.info("Processados " + count + "/" + ocorrenciasChaves.size());
				}
				
			}			
			
			//----
			
			dataRegistroSemifinal.add(Calendar.SECOND, 1); //-- para nao selecionar o ultimo registro carregado.
			loopDataRegistroInicial = dataRegistroSemifinal.getTime();
		}
			
		logger.info("Termino do processo.");
		return "COMPLETED";
	}
	
	
	private Set<OcorrenciaChave> listarChaves(String dataInicial, String dataFinal)
	{
		Client client = ClientBuilder.newClient();
		String servico = "http://"
				+ appConfig.getOcorrenciaRdoBackendHost()
				+ ":"
				+ appConfig.getOcorrenciaRdoBackendPort()
				+ "/ocorrenciardobackend/rest/"
				+ "rdoextrair/pesquisarPorDataDeRegistro/{dataRegistroInicial}/{dataRegistroFinal}";
		WebTarget webTarget = client.target(servico);
		Response response = webTarget
				.resolveTemplate("dataRegistroInicial", dataInicial)
				.resolveTemplate("dataRegistroFinal", dataFinal)
				.request(MediaType.APPLICATION_JSON)
				.get();
		String json = response.readEntity(String.class);
		if (verificarErro.contemErro(response, json))
		{
			String msg = verificarErro.criarMensagem(response, json, servico);
			logger.error(msg);
			throw new RuntimeException(msg);
		}

		Type collectionType = new TypeToken<Set<OcorrenciaChave>>(){}.getType();
		Set<OcorrenciaChave> lista = (Set<OcorrenciaChave>) converterOcorrenciaChave.paraObjeto(json, collectionType);
		return lista;
	}
	
	private void registrarChave(OcorrenciaChave ocorrenciaChave)
	{
		ControleOcorrencia controleOcorrencia = new ControleOcorrencia();
		controleOcorrencia.setIdDelegacia(ocorrenciaChave.getIdDelegacia());
		controleOcorrencia.setNumBo(ocorrenciaChave.getNumBo());
		controleOcorrencia.setAnoBo(ocorrenciaChave.getAnoBo());
		controleOcorrencia.setComplementar(ocorrenciaChave.getComplementar());
		controleOcorrencia.setDatahoraRegistroBo(ocorrenciaChave.getDatahoraRegistroBo());
		
		Client client = ClientBuilder.newClient();
		String servico = "http://" + host + ":"+ port + "/ocorrenciaespelhobackend/rest/" + 
				"espelhoOcorrenciaControle/adicionar";
		WebTarget webTarget = client.target(servico);
		
		String json = converterControleOcorrencia.paraJson(controleOcorrencia);
		Response response = webTarget
				.request(MediaType.APPLICATION_JSON)
				.put(Entity.json(json));
		json = response.readEntity(String.class);
		if (verificarErro.contemErro(response, json))
		{
			String msg = verificarErro.criarMensagem(response, json, servico);
			logger.error(msg);
			throw new RuntimeException(msg);
		}
	}
	
	
}