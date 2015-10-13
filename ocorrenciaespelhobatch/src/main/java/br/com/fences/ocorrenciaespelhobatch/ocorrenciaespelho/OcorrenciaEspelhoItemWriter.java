package br.com.fences.ocorrenciaespelhobatch.ocorrenciaespelho;

import java.util.List;

import javax.batch.api.chunk.AbstractItemWriter;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import br.com.fences.fencesutils.constante.EstadoProcessamento;
import br.com.fences.fencesutils.conversor.converter.Converter;
import br.com.fences.fencesutils.rest.tratamentoerro.util.VerificarErro;
import br.com.fences.fencesutils.verificador.Verificador;
import br.com.fences.ocorrenciaentidade.controle.ControleOcorrencia;
import br.com.fences.ocorrenciaentidade.ocorrencia.Ocorrencia;
import br.com.fences.ocorrenciaespelhobatch.config.AppConfig;
import br.com.fences.ocorrenciaespelhobatch.ocorrenciaespelho.to.OcorrenciaCompostaTO;

@Named
public class OcorrenciaEspelhoItemWriter extends AbstractItemWriter{

	@Inject
	private transient Logger logger;
	
	@Inject
	private AppConfig appConfig;
	
	@Inject
	private VerificarErro verificarErro;
	
	@Inject
	private Converter<Ocorrencia> ocorrenciaConverter;
	
	@Inject
	private Converter<ControleOcorrencia> controleOcorrenciaConverter;
	
	private String host;
	private String port;
	
	//private Gson gson = new GsonBuilder().create();
	
	@Override
	public void writeItems(List<Object> items) throws Exception {
		
		host = appConfig.getServerBackendHost();
		port = appConfig.getServerBackendPort();
		
		for (Object item : items)
		{
			OcorrenciaCompostaTO ocorrenciaCompostaTO = (OcorrenciaCompostaTO) item;
			Ocorrencia ocorrencia = ocorrenciaCompostaTO.getOcorrencia();
			
			ControleOcorrencia controleOcorrencia = ocorrenciaCompostaTO.getControleOcorrencia();
			
			logger.info("Gravando... " + msgOcorrencia(ocorrencia));
			String json = ocorrenciaConverter.paraJson(ocorrencia);
			Client client = ClientBuilder.newClient();
			String servico = "http://" + host + ":"+ port + "/ocorrenciaespelhobackend/rest/" + 
					"espelhoOcorrencia/adicionar";
			WebTarget webTarget = client
					.target(servico);
			
			//Entity.json(ocorrencia)
			Response response = webTarget
					.request(MediaType.APPLICATION_JSON)
					.put(Entity.json(json));
			json = response.readEntity(String.class);
			ocorrencia = ocorrenciaConverter.paraObjeto(json, Ocorrencia.class);
			if (verificarErro.contemErro(response, json) || !Verificador.isValorado(ocorrencia.getId()))
			{
				registrarControle(controleOcorrencia, false);
				logger.info("Ocorrencia registrada com erro.");
			}
			else
			{
				controleOcorrencia.setIdOcorrencia(ocorrencia.getId());
				registrarControle(controleOcorrencia, true);
				logger.info("Ocorrencia registrada com sucesso.");
			}
		}
		
	}
	
	private void registrarControle(ControleOcorrencia controleOcorrencia, boolean sucesso)
	{
		
		if (sucesso)
		{
			controleOcorrencia.setEstadoProcessamentoOcorrencia(EstadoProcessamento.OK);
			controleOcorrencia.setEstadoProcessamentoIndiciados(EstadoProcessamento.PROCESSAR);
		}
		else
		{
			controleOcorrencia.setEstadoProcessamentoOcorrencia(EstadoProcessamento.ERRO);
		}
		
		String json = controleOcorrenciaConverter.paraJson(controleOcorrencia);
		Client client = ClientBuilder.newClient();
		String servico = "http://" + host + ":"+ port + "/ocorrenciaespelhobackend/rest/" + 
				"espelhoOcorrencia/substituir";
		WebTarget webTarget = client.target(servico);
		
		Response response = webTarget
				.request(MediaType.APPLICATION_JSON)
				.post(Entity.json(json));
		json = response.readEntity(String.class);
		if (verificarErro.contemErro(response, json))
		{
			String msg = verificarErro.criarMensagem(response, json, servico);
			logger.error(msg);
			throw new RuntimeException(msg);
		}
	}
	
	private String msgOcorrencia(Ocorrencia ocorrencia)
	{
		String msg = ocorrencia.getNumBo() + "/" + ocorrencia.getAnoBo() + "/"
				+ ocorrencia.getIdDelegacia() + "/"
				+ ocorrencia.getNomeDelegacia() + "/"
				+ ocorrencia.getDatahoraRegistroBo();
		return msg;
	}

}
