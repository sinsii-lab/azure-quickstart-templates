%csbmva_initialize_cr(m_cr_unique_cd=RM_CONTENT);
%rmcr_init_cprm;
%let model_id_lst=21,22;
%dabt_cprm_create_mdl_exp_spec
(model_id_lst = %quote(&model_id_lst));