use borsh::BorshDeserialize;
use lastbuy_sdk::{
    ANCHOR_DISCRIMINATOR_SIZE,
    bonding_curve::{CURVE_STATE_DISCRIMINATOR, CurveState, CurveStatus, PROGRAM_ID, derive_curve},
};
use solana_pubkey::Pubkey;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CurveError {
    #[error("curve account has the wrong owner")]
    Owner,
    #[error("curve account has an invalid discriminator")]
    Discriminator,
    #[error("curve account data is invalid: {0}")]
    Decode(#[source] std::io::Error),
    #[error("curve address does not match its mint")]
    Address,
}

pub fn decode_curve(
    address: &Pubkey,
    owner: &Pubkey,
    data: &[u8],
) -> Result<CurveState, CurveError> {
    if *owner != PROGRAM_ID {
        return Err(CurveError::Owner);
    }

    let (discriminator, mut payload) = data
        .split_at_checked(ANCHOR_DISCRIMINATOR_SIZE)
        .ok_or(CurveError::Discriminator)?;

    if discriminator != CURVE_STATE_DISCRIMINATOR {
        return Err(CurveError::Discriminator);
    }

    let curve = CurveState::deserialize(&mut payload).map_err(CurveError::Decode)?;

    if derive_curve(&curve.mint).0 != *address {
        return Err(CurveError::Address);
    }

    Ok(curve)
}

pub fn eligible(curve: &CurveState) -> bool {
    matches!(
        curve.status,
        CurveStatus::Graduated | CurveStatus::Migrating
    ) && curve.migration_funder != Pubkey::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_wrong_owner_and_truncated_data() {
        assert!(matches!(
            decode_curve(&Pubkey::default(), &Pubkey::default(), &[]),
            Err(CurveError::Owner)
        ));
        assert!(matches!(
            decode_curve(&Pubkey::default(), &PROGRAM_ID, &[]),
            Err(CurveError::Discriminator)
        ));
        assert!(matches!(
            decode_curve(&Pubkey::default(), &PROGRAM_ID, &CURVE_STATE_DISCRIMINATOR),
            Err(CurveError::Decode(_))
        ));
    }
}
