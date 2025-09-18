"""
Book System Database Models
SQLAlchemy models for campaign health, risk assessment, and partner management
"""

from sqlalchemy import (
    Column, String, Integer, Boolean, DateTime, Date, Numeric,
    Text, ForeignKey, BigInteger, func
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from ..database import Base


class Campaign(Base):
    """Campaign/Account core data model."""
    __tablename__ = "campaigns"
    __table_args__ = {"schema": "book"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_id = Column(String(255), unique=True, nullable=False, index=True)
    maid = Column(String(255))
    advertiser_name = Column(String(255), index=True)
    partner_name = Column(String(255), index=True)
    bid_name = Column(String(255))
    campaign_name = Column(String(255))

    # Account Management
    am = Column(String(255))
    optimizer = Column(String(255))
    gm = Column(String(255))
    business_category = Column(String(255))

    # Financial Data
    campaign_budget = Column(Numeric(12, 2))
    amount_spent = Column(Numeric(12, 2))

    # Operational Metrics
    io_cycle = Column(Integer)
    avg_cycle_length = Column(Numeric(8, 2))
    days_elapsed = Column(Integer)
    days_active = Column(Integer)
    utilization = Column(Numeric(5, 2))

    # Lead Metrics
    running_cid_leads = Column(Integer)
    running_cid_cpl = Column(Numeric(10, 2))
    cpl_goal = Column(Numeric(10, 2))
    bsc_cpl_avg = Column(Numeric(10, 2))
    effective_cpl_goal = Column(Numeric(10, 2))
    expected_leads_monthly = Column(Integer)
    expected_leads_to_date = Column(Integer)
    expected_leads_to_date_spend = Column(Numeric(12, 2))

    # Runtime Fields
    true_days_running = Column(Integer)
    true_months_running = Column(Numeric(4, 2))
    cycle_label = Column(String(100))

    # Risk Scores
    age_risk = Column(Numeric(5, 2))
    lead_risk = Column(Numeric(5, 2))
    cpl_risk = Column(Numeric(5, 2))
    util_risk = Column(Numeric(5, 2))
    structure_risk = Column(Numeric(5, 2))
    total_risk_score = Column(Numeric(5, 2))
    value_score = Column(Numeric(5, 2))
    final_priority_score = Column(Numeric(5, 2))

    # Priority Classification
    priority_index = Column(Numeric(8, 4), index=True)
    priority_tier = Column(String(50), index=True)
    primary_issue = Column(String(255))

    # Churn Risk Data
    churn_prob_90d = Column(Numeric(5, 4), index=True)
    churn_risk_band = Column(String(50))
    revenue_at_risk = Column(Numeric(12, 2))
    risk_drivers_json = Column(JSONB)

    # FLARE Scoring
    flare_score = Column(Numeric(8, 4))
    flare_band = Column(String(50))
    flare_breakdown_json = Column(JSONB)
    flare_score_raw = Column(Numeric(8, 4))

    # Diagnosis
    headline_diagnosis = Column(Text)
    headline_severity = Column(String(50))
    diagnosis_pills = Column(JSONB)

    # Account Structure
    campaign_count = Column(Integer)
    true_product_count = Column(Integer)
    is_safe = Column(Boolean)

    # Goal Advice
    goal_advice_json = Column(JSONB)

    # UI State
    status = Column(String(50), default='new', index=True)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            'campaign_id': self.campaign_id,
            'maid': self.maid,
            'advertiser_name': self.advertiser_name,
            'partner_name': self.partner_name,
            'bid_name': self.bid_name,
            'campaign_name': self.campaign_name,
            'am': self.am,
            'optimizer': self.optimizer,
            'gm': self.gm,
            'business_category': self.business_category,
            'campaign_budget': float(self.campaign_budget) if self.campaign_budget else None,
            'amount_spent': float(self.amount_spent) if self.amount_spent else None,
            'io_cycle': self.io_cycle,
            'avg_cycle_length': float(self.avg_cycle_length) if self.avg_cycle_length else None,
            'days_elapsed': self.days_elapsed,
            'days_active': self.days_active,
            'utilization': float(self.utilization) if self.utilization else None,
            'running_cid_leads': self.running_cid_leads,
            'running_cid_cpl': float(self.running_cid_cpl) if self.running_cid_cpl else None,
            'cpl_goal': float(self.cpl_goal) if self.cpl_goal else None,
            'bsc_cpl_avg': float(self.bsc_cpl_avg) if self.bsc_cpl_avg else None,
            'effective_cpl_goal': float(self.effective_cpl_goal) if self.effective_cpl_goal else None,
            'expected_leads_monthly': self.expected_leads_monthly,
            'expected_leads_to_date': self.expected_leads_to_date,
            'expected_leads_to_date_spend': float(self.expected_leads_to_date_spend) if self.expected_leads_to_date_spend else None,
            'true_days_running': self.true_days_running,
            'true_months_running': float(self.true_months_running) if self.true_months_running else None,
            'cycle_label': self.cycle_label,
            'age_risk': float(self.age_risk) if self.age_risk else None,
            'lead_risk': float(self.lead_risk) if self.lead_risk else None,
            'cpl_risk': float(self.cpl_risk) if self.cpl_risk else None,
            'util_risk': float(self.util_risk) if self.util_risk else None,
            'structure_risk': float(self.structure_risk) if self.structure_risk else None,
            'total_risk_score': float(self.total_risk_score) if self.total_risk_score else None,
            'value_score': float(self.value_score) if self.value_score else None,
            'final_priority_score': float(self.final_priority_score) if self.final_priority_score else None,
            'priority_index': float(self.priority_index) if self.priority_index else None,
            'priority_tier': self.priority_tier,
            'primary_issue': self.primary_issue,
            'churn_prob_90d': float(self.churn_prob_90d) if self.churn_prob_90d else None,
            'churn_risk_band': self.churn_risk_band,
            'revenue_at_risk': float(self.revenue_at_risk) if self.revenue_at_risk else None,
            'risk_drivers_json': self.risk_drivers_json,
            'flare_score': float(self.flare_score) if self.flare_score else None,
            'flare_band': self.flare_band,
            'flare_breakdown_json': self.flare_breakdown_json,
            'flare_score_raw': float(self.flare_score_raw) if self.flare_score_raw else None,
            'headline_diagnosis': self.headline_diagnosis,
            'headline_severity': self.headline_severity,
            'diagnosis_pills': self.diagnosis_pills,
            'campaign_count': self.campaign_count,
            'true_product_count': self.true_product_count,
            'is_safe': self.is_safe,
            'goal_advice_json': self.goal_advice_json,
            'status': self.status,
            'risk_model_version': getattr(self, 'risk_model_version', 'unknown'),
            'risk_constants': {}  # Default empty constants
        }


class Partner(Base):
    """Partners summary data for growth dashboard."""
    __tablename__ = "partners"
    __table_args__ = {"schema": "book"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    partner_name = Column(String(255), unique=True, nullable=False, index=True)
    playbook = Column(String(100), default='seo_dash')

    # Metrics
    total_budget = Column(Numeric(12, 2))
    single_product_count = Column(Integer, default=0)
    two_product_count = Column(Integer, default=0)
    three_plus_product_count = Column(Integer, default=0)
    cross_sell_ready_count = Column(Integer, default=0)
    upsell_ready_count = Column(Integer, default=0)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    opportunities = relationship("PartnerOpportunity", back_populates="partner")

    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            'partner': self.partner_name,
            'metrics': {
                'budget': float(self.total_budget) if self.total_budget else 0,
                'singleCount': self.single_product_count,
                'twoCount': self.two_product_count,
                'threePlusCount': self.three_plus_product_count,
                'crossReadyCount': self.cross_sell_ready_count,
                'upsellReadyCount': self.upsell_ready_count
            }
        }


class PartnerOpportunity(Base):
    """Partner opportunities detail data."""
    __tablename__ = "partner_opportunities"
    __table_args__ = {"schema": "book"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    partner_id = Column(UUID(as_uuid=True), ForeignKey('book.partners.id'), nullable=False)
    partner_name = Column(String(255), nullable=False, index=True)
    playbook = Column(String(100), default='seo_dash')

    # Opportunity Groups (stored as JSONB for flexibility)
    single_ready = Column(JSONB)  # Single product advertisers ready for cross-sell
    two_ready = Column(JSONB)     # Two product advertisers
    three_plus_ready = Column(JSONB)  # Three+ product advertisers
    scale_ready = Column(JSONB)   # Campaigns ready for budget increase
    too_low = Column(JSONB)       # Budget inadequate campaigns

    # Playbook Configuration
    playbook_config = Column(JSONB)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    partner = relationship("Partner", back_populates="opportunities")

    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            'partner': self.partner_name,
            'playbook': {
                'label': self.playbook.replace('_', ' ').title(),
                'elements': ['Search', 'SEO', 'DASH'],  # Default elements
                'min_sem': 2500
            },
            'groups': {
                'singleReady': self.single_ready or [],
                'twoReady': self.two_ready or [],
                'threePlusReady': self.three_plus_ready or [],
                'scaleReady': self.scale_ready or [],
                'tooLow': self.too_low or []
            }
        }


class DataSnapshot(Base):
    """Data freshness tracking."""
    __tablename__ = "data_snapshots"
    __table_args__ = {"schema": "book"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_date = Column(Date, nullable=False, index=True)
    file_name = Column(String(255))
    file_size_bytes = Column(BigInteger)
    record_count = Column(Integer)
    last_modified = Column(DateTime(timezone=True))
    is_current = Column(Boolean, default=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            'data_snapshot_date': self.snapshot_date.isoformat() if self.snapshot_date else None,
            'last_modified': self.last_modified.isoformat() if self.last_modified else None,
            'last_modified_display': self.last_modified.strftime("%Y-%m-%d %H:%M:%S") if self.last_modified else "Unknown",
            'file_name': self.file_name or "Unknown",
            'file_size_bytes': self.file_size_bytes or 0,
            'record_count': self.record_count or 0,
            'is_current': self.is_current or False
        }