package br.com.fences.ocorrenciaespelhobatch.ocorrenciaespelho;

import javax.batch.api.partition.PartitionMapper;
import javax.batch.api.partition.PartitionPlan;
import javax.inject.Inject;
import javax.inject.Named;

@Named
public class OcorrenciaEspelhoPartitionMapper implements PartitionMapper{

	@Inject
	private OcorrenciaEspelhoPartitionPlan ocorrenciaEspelhoPartitionPlan;

	
	@Override
	public PartitionPlan mapPartitions() throws Exception {
		return ocorrenciaEspelhoPartitionPlan;
	}

}
