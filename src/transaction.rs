//! Atomic v1 migration instruction construction. Callers must supply freshly validated accounts.
use lastbuy_sdk::{
    SPL_TOKEN_2022_PROGRAM_ID, SPL_TOKEN_PROGRAM_ID, WSOL_ID,
    accounts::{CurveState, GlobalConfig},
    instructions::{
        MigrateInstruction, MigrateInstructionCtx, PrepareMigrationInstruction,
        PrepareMigrationInstructionCtx,
    },
    pda::{derive_curve, derive_pool_creator},
    types::CurveStatus,
};
use solana_instruction::Instruction;
use solana_keypair::{Keypair, Signer};
use solana_message::{
    VersionedMessage,
    v1::{Message, TransactionConfig},
};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("curve address does not match its mint")]
    CurveMismatch,
    #[error("curve is not eligible for migration")]
    Ineligible,
    #[error("curve migration is already finalized on chain")]
    AlreadyMigrated,
    #[error("required migration instruction is paused")]
    Paused,
    #[error("resource limits must be positive and priority fee must not exceed its cap")]
    InvalidPolicy,
    #[error("could not compile v1 message: {0}")]
    Compile(#[source] solana_message::CompileError),
    #[error("could not sign transaction: {0}")]
    Sign(#[source] solana_signer::SignerError),
}

#[derive(Debug, Clone, Copy)]
pub struct ResourcePolicy {
    pub compute_unit_limit: u32,
    pub loaded_accounts_data_size_limit: u32,
    pub priority_fee_lamports: u64,
    pub max_priority_fee_lamports: u64,
}

pub fn build_instruction_plan(
    payer: Pubkey,
    curve: Pubkey,
    state: &CurveState,
    config: &GlobalConfig,
) -> Result<Vec<Instruction>, BuildError> {
    if derive_curve(&state.mint).0 != curve {
        return Err(BuildError::CurveMismatch);
    }
    if state.status == CurveStatus::Migrated {
        return Err(BuildError::AlreadyMigrated);
    }
    if !matches!(
        state.status,
        CurveStatus::Graduated | CurveStatus::Migrating
    ) || state.migration_funder == Pubkey::default()
    {
        return Err(BuildError::Ineligible);
    }
    if config.admin.is_migrate_disabled
        || (state.status == CurveStatus::Graduated && config.admin.is_prepare_migration_disabled)
    {
        return Err(BuildError::Paused);
    }
    let pool_creator = derive_pool_creator(&state.migration_funder, &curve).0;
    let mut instructions = vec![
        spl_associated_token_account_interface::instruction::create_associated_token_account_idempotent(&payer, &pool_creator, &WSOL_ID, &SPL_TOKEN_PROGRAM_ID),
        spl_associated_token_account_interface::instruction::create_associated_token_account_idempotent(&payer, &pool_creator, &state.mint, &SPL_TOKEN_2022_PROGRAM_ID),
    ];
    if state.status == CurveStatus::Graduated {
        instructions.push(PrepareMigrationInstruction::new(
            PrepareMigrationInstructionCtx {
                user: payer,
                migration_funder: state.migration_funder,
                creator: state.creator,
                mint: state.mint,
                fee_recipient_1: config.fees.fee_recipient_1,
                fee_recipient_2: config.fees.fee_recipient_2,
            },
        ));
    }
    instructions.push(MigrateInstruction::new(MigrateInstructionCtx {
        user: payer,
        migration_funder: state.migration_funder,
        mint: state.mint,
        raydium_amm_config: config.raydium.raydium_amm_config,
        raydium_fee_account: config.raydium.raydium_fee_account,
        raydium_cpmm_program: config.raydium.raydium_cpmm_program,
        raydium_lock_program: config.raydium.raydium_lock_program,
    }));
    Ok(instructions)
}

pub fn sign_transaction(
    instructions: &[Instruction],
    payer: &Keypair,
    recent_blockhash: solana_hash::Hash,
    policy: ResourcePolicy,
) -> Result<VersionedTransaction, BuildError> {
    if policy.compute_unit_limit == 0
        || policy.loaded_accounts_data_size_limit == 0
        || policy.priority_fee_lamports > policy.max_priority_fee_lamports
    {
        return Err(BuildError::InvalidPolicy);
    }
    let config = TransactionConfig::empty()
        .with_compute_unit_limit(policy.compute_unit_limit)
        .with_loaded_accounts_data_size_limit(policy.loaded_accounts_data_size_limit)
        .with_priority_fee(policy.priority_fee_lamports);
    let message =
        Message::try_compile_with_config(&payer.pubkey(), instructions, recent_blockhash, config)
            .map_err(BuildError::Compile)?;
    VersionedTransaction::try_new(VersionedMessage::V1(message), &[payer]).map_err(BuildError::Sign)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lastbuy_sdk::types::{AdminConfig, FeeConfig, ProtocolConfig, RaydiumConfig};

    fn fixtures() -> (Pubkey, CurveState, GlobalConfig) {
        let mint = Pubkey::new_unique();
        let state = CurveState {
            bump: 0,
            status: CurveStatus::Graduated,
            migration_funder: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            mint,
            last_buyer: Pubkey::default(),
            last_buy_sol: 0,
            last_buy_ts: 0,
            max_hold_duration_secs: 0,
            max_duration_holder: Pubkey::default(),
            real_reserves_sol: 0,
            real_reserves_tokens: 0,
            virtual_reserves_sol: 0,
            virtual_reserves_tokens: 0,
            last_buy_timer_secs_snapshot: 0,
            migration_threshold_sol_snapshot: 0,
        };
        let config = GlobalConfig {
            protocol_version: 1,
            bump: 0,
            fees: FeeConfig {
                protocol_curve_fee_bps: 0,
                protocol_claim_creator_fee_bps: 0,
                creator_fee_bps: 0,
                last_buyer_fee_bps: 0,
                fee_recipient_1: Pubkey::new_unique(),
                fee_recipient_2: Pubkey::new_unique(),
            },
            protocol: ProtocolConfig {
                last_buy_timer_secs: 0,
                migration_threshold_sol: 0,
                protocol_authority: Pubkey::default(),
            },
            raydium: RaydiumConfig {
                raydium_cpmm_program: Pubkey::new_unique(),
                raydium_lock_program: Pubkey::new_unique(),
                raydium_amm_config: Pubkey::new_unique(),
                raydium_fee_account: Pubkey::new_unique(),
                raydium_cpmm_migration_coverage_sol: 0,
            },
            admin: AdminConfig::default(),
        };
        (derive_curve(&mint).0, state, config)
    }

    #[test]
    fn instructions_match_sdk_and_backend_order_for_both_statuses() {
        let (curve, mut state, config) = fixtures();
        let payer = Pubkey::new_unique();
        let pool_creator = derive_pool_creator(&state.migration_funder, &curve).0;
        let expected_atas = [
            spl_associated_token_account_interface::instruction::create_associated_token_account_idempotent(&payer, &pool_creator, &WSOL_ID, &SPL_TOKEN_PROGRAM_ID),
            spl_associated_token_account_interface::instruction::create_associated_token_account_idempotent(&payer, &pool_creator, &state.mint, &SPL_TOKEN_2022_PROGRAM_ID),
        ];
        let prepare = PrepareMigrationInstruction::new(PrepareMigrationInstructionCtx {
            user: payer,
            migration_funder: state.migration_funder,
            creator: state.creator,
            mint: state.mint,
            fee_recipient_1: config.fees.fee_recipient_1,
            fee_recipient_2: config.fees.fee_recipient_2,
        });
        let migrate = MigrateInstruction::new(MigrateInstructionCtx {
            user: payer,
            migration_funder: state.migration_funder,
            mint: state.mint,
            raydium_amm_config: config.raydium.raydium_amm_config,
            raydium_fee_account: config.raydium.raydium_fee_account,
            raydium_cpmm_program: config.raydium.raydium_cpmm_program,
            raydium_lock_program: config.raydium.raydium_lock_program,
        });
        let graduated = build_instruction_plan(payer, curve, &state, &config).unwrap();
        assert_eq!(
            graduated,
            vec![
                expected_atas[0].clone(),
                expected_atas[1].clone(),
                prepare,
                migrate.clone()
            ]
        );
        assert_eq!(graduated[2].accounts.len(), 18);
        assert_eq!(graduated[3].accounts.len(), 35);
        assert_eq!(
            graduated[2].data,
            lastbuy_sdk::PREPARE_MIGRATION_DISCRIMINATOR
        );
        assert_eq!(graduated[3].data, lastbuy_sdk::MIGRATE_DISCRIMINATOR);
        state.status = CurveStatus::Migrating;
        assert_eq!(
            build_instruction_plan(payer, curve, &state, &config).unwrap(),
            vec![expected_atas[0].clone(), expected_atas[1].clone(), migrate]
        );
    }

    #[test]
    fn signs_a_v1_transaction_with_configured_priority_fee() {
        let (curve, state, config) = fixtures();
        let payer = Keypair::new();
        let instructions = build_instruction_plan(payer.pubkey(), curve, &state, &config).unwrap();
        let tx = sign_transaction(
            &instructions,
            &payer,
            solana_hash::Hash::default(),
            ResourcePolicy {
                compute_unit_limit: 1_000_000,
                loaded_accounts_data_size_limit: 1_000_000,
                priority_fee_lamports: 1_000,
                max_priority_fee_lamports: 10_000,
            },
        )
        .unwrap();
        assert_eq!(tx.signatures.len(), 1);
        assert!(matches!(tx.message, VersionedMessage::V1(_)));
        let bytes = wincode::serialize(&tx).unwrap();
        let decoded: VersionedTransaction = wincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, tx);
        assert_eq!(wincode::serialize(&decoded).unwrap(), bytes);
    }
}
